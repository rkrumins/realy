package io.realy.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.contracts.scoring.v1.DecisionRecord;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The audit write-ahead log: durable only when on the broker or fsynced; replayed without loss or repeats. */
class DecisionSinkTest {
    static DecisionRecord rec(long id) {
        return DecisionRecord.newBuilder().setEventId(id).setEvent(TransactionEvent.newBuilder().setEventId(id).setCustomerId(7)).build();
    }

    static MockProducer<byte[], byte[]> producer(boolean autoComplete) {
        return new MockProducer<>(autoComplete, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void spooledRecordsReachTheBrokerOnceInOrder(@TempDir Path dir) throws Exception {
        Path spool = dir.resolve("audit.spool");
        MockProducer<byte[], byte[]> p = producer(true);
        try (DecisionSink s = new DecisionSink(p, 0, spool)) {
            for (long i = 1; i <= 5; i++) assertThat(s.spool(rec(i))).isTrue();
            s.replay();
            assertThat(p.history()).extracting(r -> DecisionRecord.parseFrom(r.value()).getEventId()).containsExactly(1L, 2L, 3L, 4L, 5L);
            assertThat(Files.exists(spool)).isFalse();
            assertThat(Files.exists(Path.of(spool + ".replaying"))).isFalse();
            s.replay();                                     // nothing left: nothing re-sent
            assertThat(p.history()).hasSize(5);
        }
    }

    @Test
    void aTornTailIsCutOffAndTheRestReplayed(@TempDir Path dir) throws Exception {
        Path spool = dir.resolve("audit.spool");
        MockProducer<byte[], byte[]> p = producer(true);
        try (DecisionSink s = new DecisionSink(producer(true), 0, spool)) {   // spools only (the broker was down)
            s.spool(rec(1));
            s.spool(rec(2));
        }
        // a crash mid-write: half a record header at the end
        Files.write(spool, new byte[] {0, 0, 0}, StandardOpenOption.APPEND);
        try (DecisionSink s = new DecisionSink(p, 0, spool)) {
            s.replay();
        }
        assertThat(p.history()).hasSize(2);
    }

    @Test
    void aCrashMidReplayResumesFromItsPosition(@TempDir Path dir) throws Exception {
        Path spool = dir.resolve("audit.spool");
        MockProducer<byte[], byte[]> ok = producer(true);
        try (DecisionSink s = new DecisionSink(producer(true), 0, spool)) {
            for (long i = 1; i <= 3; i++) s.spool(rec(i));
        }
        // the first record was shipped and its position recorded, then the process died
        Files.move(spool, Path.of(spool + ".replaying"));
        long first = 8 + rec(1).toByteArray().length;
        Files.writeString(Path.of(spool + ".replaying.pos"), Long.toString(first));
        try (DecisionSink s = new DecisionSink(ok, 0, spool)) {
            s.replay();
        }
        assertThat(ok.history()).extracting(r -> DecisionRecord.parseFrom(r.value()).getEventId()).containsExactly(2L, 3L);
    }

    @Test
    void persistCompletesOnlyWhenSomethingDurableTookTheRecord(@TempDir Path dir) throws Exception {
        // the spool path is a directory: the spool cannot take records; the broker answers later
        Path notAFile = Files.createDirectory(dir.resolve("spool-is-a-dir"));
        MockProducer<byte[], byte[]> p = producer(false);
        try (DecisionSink s = new DecisionSink(p, 0, notAFile)) {
            var f = s.persist(rec(9));
            Thread.sleep(300);
            assertThat(f).isNotDone();                      // neither durable yet: the offset stays held
            for (int i = 0; i < 50 && p.history().isEmpty(); i++) Thread.sleep(100);
            p.completeNext();                               // the broker acknowledges the retry
            f.get(10, TimeUnit.SECONDS);
            assertThat(f).isDone();
        }
    }
}
