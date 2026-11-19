package io.realy.emitter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.realy.common.BenchClock;
import io.realy.common.Env;
import io.realy.common.ProtoJson;
import io.realy.common.StageMetrics;
import io.realy.contracts.bench.v1.BenchTrace;
import io.realy.contracts.bench.v1.Span;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.ScoreRequest;
import io.realy.contracts.scoring.v1.ScoreResponse;
import io.realy.contracts.scoring.v1.ScoringServiceGrpc;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

/**
 * Open-loop gRPC load emitter for the synchronous rail. Replays a compiled dataset (realy world
 * compile / the runner) so the sync and async paths see byte-identical events. Each request is sent
 * at its intended time (derived from event time and the run's time scale) and latency is measured
 * from that intended time, so a slow server cannot slow the schedule down (no coordinated omission).
 *
 * Output (--out): magic "RLYGR\0\1\0", u64 n, then n fixed records (little-endian):
 *   u64 event_id, i64 t_intended, i64 t_sent, i64 t_response, i64 t_ingest, i64 t_decision,
 *   i8 outcome, i8 path, u8 degraded, u8 status (0 ok, 1 deadline, 2 error, 3 no response),
 *   i32[16] stage durations (us, -1 absent) in STAGES order.
 */
public final class LoadEmitterMain {
    static final int[] STAGES = {11, 12, 20, 21, 22, 30, 31, 40, 41, 42, 43, 50, 51, 60, 61, 81};
    static final int REC = 8 * 6 + 4 + 4 * STAGES.length;

    public static void main(String[] a) throws Exception {
        Map<String, String> args = parse(a);
        Path dataset = Path.of(args.get("dataset"));
        String target = args.getOrDefault("target", "localhost:8080");
        String runId = args.getOrDefault("run-id", "");
        long t0 = Long.parseLong(args.getOrDefault("start-at-us", Long.toString(BenchClock.nowUs() + 2_000_000)));
        double timeScale = Double.parseDouble(args.get("time-scale"));
        long simStart = Long.parseLong(args.get("sim-start-us"));
        int channels = Integer.parseInt(args.getOrDefault("channels", "4"));
        int deadlineMs = Integer.parseInt(args.getOrDefault("deadline-ms", "0"));
        long callTimeoutMs = Long.parseLong(args.getOrDefault("call-timeout-ms", "5000"));
        boolean stream = "stream".equals(args.getOrDefault("mode", "unary"));
        Path out = Path.of(args.get("out"));

        // ---------------------------------------------------------------- load dataset
        byte[][] payloads;
        long[] eventIds;
        long[] offsets;
        try (var in = new DataInputStream(new java.io.BufferedInputStream(Files.newInputStream(dataset), 1 << 20))) {
            byte[] magic = in.readNBytes(8);
            if (magic[0] != 'R' || magic[3] != 'D') throw new IOException("not a realy dataset");
            long n = Long.reverseBytes(in.readLong());
            payloads = new byte[(int) n][];
            eventIds = new long[(int) n];
            offsets = new long[(int) n];
            for (int i = 0; i < n; i++) {
                int len = Integer.reverseBytes(in.readInt());
                payloads[i] = in.readNBytes(len);
                TransactionEvent e = TransactionEvent.parseFrom(payloads[i]);
                eventIds[i] = e.getEventId();
                offsets[i] = (long) ((e.getEventTimeUs() - simStart) / timeScale);
            }
        }
        int n = payloads.length;
        System.out.printf("loaded %,d events; schedule %.1f s; target %s (%s)%n", n,
                n == 0 ? 0 : offsets[n - 1] / 1e6, target, stream ? "bidi stream" : "unary");

        // ---------------------------------------------------------------- results
        long[] tSent = new long[n];
        long[] tResp = new long[n];
        long[] tIngest = new long[n];
        long[] tDecision = new long[n];
        byte[] outcome = new byte[n];
        byte[] path = new byte[n];
        byte[] degraded = new byte[n];
        byte[] status = new byte[n];
        int[][] stage = new int[STAGES.length][n];
        for (int[] s : stage) java.util.Arrays.fill(s, -1);
        java.util.Arrays.fill(status, (byte) 3);
        Map<Integer, Integer> stageIdx = new HashMap<>();
        for (int i = 0; i < STAGES.length; i++) stageIdx.put(STAGES[i], i);
        AtomicInteger outstanding = new AtomicInteger();
        AtomicLong errors = new AtomicLong();
        StageMetrics metrics = new StageMetrics("emitter", "grpc-" + Env.str("HOSTNAME", "0"));
        metrics.setRunId(runId);
        metrics.startPublishing();

        ManagedChannel[] chans = new ManagedChannel[channels];
        ScoringServiceGrpc.ScoringServiceStub[] stubs = new ScoringServiceGrpc.ScoringServiceStub[channels];
        for (int i = 0; i < channels; i++) {
            chans[i] = ManagedChannelBuilder.forTarget(target).usePlaintext().maxInboundMessageSize(16 << 20).build();
            stubs[i] = ScoringServiceGrpc.newStub(chans[i]);
        }
        Map<Long, Integer> indexOf = new HashMap<>(n * 2);
        for (int i = 0; i < n; i++) indexOf.put(eventIds[i], i);

        final class Handler {
            void ok(int i, ScoreResponse r) {
                long now = BenchClock.nowUs();
                tResp[i] = now;
                outcome[i] = (byte) r.getOutcomeValue();
                path[i] = (byte) r.getPathValue();
                degraded[i] = (byte) (r.getDegraded() ? 1 : 0);
                status[i] = 0;
                for (Span s : r.getTrace().getSpansList()) {
                    int st = s.getStageValue();
                    if (st == 10) tIngest[i] = s.getStartUs();
                    else if (st == 70) tDecision[i] = s.getStartUs();
                    Integer k = stageIdx.get(st);
                    if (k != null) stage[k][i] = (st == 20 || st == 21 || st == 81) && stage[k][i] > 0
                            ? stage[k][i] + s.getDurUs() : s.getDurUs();
                }
                metrics.recordUs("e2e", now - (t0 + offsets[i]));
                metrics.inc("responses");
                outstanding.decrementAndGet();
            }

            void fail(int i, Throwable t) {
                tResp[i] = BenchClock.nowUs();
                Status.Code c = Status.fromThrowable(t).getCode();
                status[i] = (byte) (c == Status.Code.DEADLINE_EXCEEDED ? 1 : 2);
                errors.incrementAndGet();
                metrics.inc("errors");
                outstanding.decrementAndGet();
            }
        }
        Handler h = new Handler();

        @SuppressWarnings("unchecked")
        StreamObserver<ScoreRequest>[] streams = new StreamObserver[channels];
        if (stream) {
            for (int c = 0; c < channels; c++) {
                streams[c] = stubs[c].scoreStream(new StreamObserver<>() {
                    @Override
                    public void onNext(ScoreResponse r) {
                        Integer i = indexOf.get(r.getEventId());
                        if (i != null) h.ok(i, r);
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("stream error: " + t);
                    }

                    @Override
                    public void onCompleted() { }
                });
            }
        }

        // ---------------------------------------------------------------- open-loop schedule
        int driver = BenchClock.domain();
        CRC32 crc = new CRC32();
        for (int i = 0; i < n; i++) {
            long due = t0 + offsets[i];
            long wait;
            while ((wait = due - BenchClock.nowUs()) > 0) {
                if (wait > 200) LockSupport.parkNanos((wait - 100) * 1000); else Thread.onSpinWait();
            }
            long sent = BenchClock.nowUs();
            crc.reset();
            crc.update(payloads[i]);
            long eid = eventIds[i];
            BenchTrace tr = BenchTrace.newBuilder().setRunId(runId).setGenId((int) (eid >>> 48)).setSeq(eid & 0xFFFFFFFFFFFFL)
                    .setTIntendedUs(due).setTSentUs(sent).setDriverDomain(driver).setPayloadCrc32((int) crc.getValue()).build();
            ScoreRequest req = ScoreRequest.newBuilder().setEvent(TransactionEvent.parseFrom(payloads[i])).setTrace(tr)
                    .setDeadlineMs(deadlineMs).build();
            tSent[i] = sent;
            metrics.recordUs("schedule_lag", sent - due);
            metrics.inc("emitted");
            outstanding.incrementAndGet();
            int c = i % channels;
            if (stream) {
                synchronized (streams[c]) {
                    streams[c].onNext(req);
                }
            } else {
                final int idx = i;
                stubs[c].withDeadlineAfter(callTimeoutMs, TimeUnit.MILLISECONDS).score(req, new StreamObserver<>() {
                    @Override
                    public void onNext(ScoreResponse r) {
                        h.ok(idx, r);
                    }

                    @Override
                    public void onError(Throwable t) {
                        h.fail(idx, t);
                    }

                    @Override
                    public void onCompleted() { }
                });
            }
        }
        long drainUntil = System.currentTimeMillis() + callTimeoutMs + 5000;
        while (outstanding.get() > 0 && System.currentTimeMillis() < drainUntil) Thread.sleep(50);
        if (stream) for (var s : streams) s.onCompleted();

        // ---------------------------------------------------------------- write results
        ByteBuffer rec = ByteBuffer.allocate(REC).order(ByteOrder.LITTLE_ENDIAN);
        try (var os = new BufferedOutputStream(new FileOutputStream(out.toFile()), 1 << 20)) {
            ByteBuffer head = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
            head.put(new byte[] {'R', 'L', 'Y', 'G', 'R', 0, 1, 0}).putLong(n);
            os.write(head.array());
            for (int i = 0; i < n; i++) {
                rec.clear();
                rec.putLong(eventIds[i]).putLong(t0 + offsets[i]).putLong(tSent[i]).putLong(tResp[i])
                        .putLong(tIngest[i]).putLong(tDecision[i]).put(outcome[i]).put(path[i]).put(degraded[i]).put(status[i]);
                for (int[] s : stage) rec.putInt(s[i]);
                os.write(rec.array());
            }
        }
        int unanswered = outstanding.get();
        String summary = String.format("{\"emitted\": %d, \"errors\": %d, \"unanswered\": %d, \"mode\": \"%s\"}",
                n, errors.get(), unanswered, stream ? "stream" : "unary");
        Files.writeString(Path.of(out + ".json"), summary);
        System.out.println(summary);
        metrics.close();
        for (ManagedChannel c : chans) c.shutdownNow();
        System.exit(0);
    }

    static Map<String, String> parse(String[] a) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < a.length; i += 2) m.put(a[i].replaceFirst("^--", ""), a[i + 1]);
        return m;
    }

    @SuppressWarnings("unused")
    private static String json(Object o) {
        return o instanceof com.google.protobuf.MessageOrBuilder m ? ProtoJson.print(m) : String.valueOf(o);
    }
}
