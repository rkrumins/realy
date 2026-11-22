package io.realy.scoring.binding;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.realy.contracts.flows.v1.TxnContext;
import io.realy.contracts.flows.v1.TxnState;
import io.realy.contracts.payments.v1.TransactionEvent;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Assembles one transaction contract's messages into the unified event (a pure state machine over
 * {@link TxnState}, so the Flink operator keeps it in keyed state and tests drive it directly). Mirrors
 * the Python twin (python/realy/bindings/transactions.py `Assembler`) rule for rule:
 * <ul>
 *   <li>a transaction is assembled when its trigger and every required part have arrived, with the optional
 *       parts that arrived before them (in the partition's order);</li>
 *   <li>a required part missing past the wait (the next message of the customer later than the trigger's
 *       event time + wait, or the wall-clock wait expiring first) assembles it without that part
 *       (partial);</li>
 *   <li>lifecycle messages after assembly move its stage; a scored stage is decided again (read-only:
 *       the features read it without counting the transaction twice); one that arrives before assembly is
 *       held and applied right after it.</li>
 * </ul>
 */
public final class Assembly {
    /** An event to score or fold: the unified event, its context and how it was made. */
    public record Out(String kind, TransactionEvent event, TxnContext ctx, boolean readOnly, byte[] trace,
                      long ingestUs, boolean backfill) {}

    public final TransactionContract contract;
    private final Map<String, WireCodec> codecs = new HashMap<>();
    private final Descriptors.Descriptor into = TransactionEvent.getDescriptor();
    private final StageMatcher matcher;

    /** Evaluates a stage's `when` (CEL over the stage message's view); supplied by the caller (rules module). */
    public interface StageMatcher {
        boolean matches(String expr, Map<String, Object> eventView);
    }

    public Assembly(TransactionContract contract, StageMatcher matcher) {
        this.contract = contract;
        this.matcher = matcher;
        contract.roles.forEach((role, type) -> codecs.put(role, WireCodec.of(EventBinding.load(type).message)));
    }

    public WireCodec codec(String role) {
        return codecs.get(role);
    }

    public long waitUs() {
        return contract.waitMs * 1000L;
    }

    // ---------------------------------------------------------------------------------------- input
    /**
     * One message of the transaction. {@code st} is its state (new: an empty builder with the link id);
     * returns the events it produces, in order.
     */
    public List<Out> onPart(TxnState.Builder st, String role, byte[] payload, long eventTimeUs, byte[] trace,
                            long ingestUs, long nowWallUs, boolean backfill) {
        List<Out> out = new ArrayList<>();
        if (st.getTransaction().isEmpty()) st.setTransaction(contract.name);
        TxnState.Part.Builder pb = TxnState.Part.newBuilder().setRole(role).setPayload(ByteString.copyFrom(payload))
                .setEventTimeUs(eventTimeUs);
        if (trace != null) pb.setTrace(ByteString.copyFrom(trace)).setIngestUs(ingestUs);
        TxnState.Part part = pb.build();
        if (st.getAssembled() && contract.isStageRole(role)) {
            record(st, part.toBuilder().clearTrace().build());
            out.addAll(stage(st, role, payload, trace, ingestUs));
            return out;
        }
        if (!st.getAssembled() && !contract.isAssemblyRole(role)) {
            st.addHeld(part);            // a lifecycle message before assembly: applied right after it
            return out;
        }
        part = part.toBuilder().clearTrace().build();   // only a stage needs its own trace
        record(st, part);
        if (st.getAssembled()) return out;   // a late part: later stages' context sees it
        if (role.equals(contract.trigger)) {
            st.setTriggerTimeUs(eventTimeUs);
            st.setDeadlineWallUs(nowWallUs + waitUs());
            if (trace != null) st.setTriggerTrace(ByteString.copyFrom(trace));
            st.setTriggerIngestUs(ingestUs);
            st.setBackfill(backfill);
        }
        int need = contract.bit(contract.trigger);
        for (String r : contract.require) need |= contract.bit(r);
        if (st.getTriggerTimeUs() != 0 && (st.getArrived() & need) == need) out.addAll(assemble(st));
        return out;
    }

    /** Whether a message of the same customer at {@code eventTimeUs} closes the wait (event-time deadline). */
    public boolean overdue(TxnState st, long eventTimeUs) {
        return !st.getAssembled() && st.getTriggerTimeUs() != 0 && eventTimeUs > st.getTriggerTimeUs() + waitUs();
    }

    /** The wait for required parts is over: assemble without them. */
    public List<Out> onDeadline(TxnState.Builder st) {
        if (st.getAssembled() || st.getTriggerTimeUs() == 0) return List.of();
        st.setPartial(true);
        return assemble(st);
    }

    /** A terminal stage was reached: the transaction's state can go. */
    public boolean finished(TxnState st) {
        for (TransactionContract.Stage s : contract.stages) if (s.terminal() && s.name().equals(st.getStage())) return true;
        return false;
    }

    // ---------------------------------------------------------------------------------------- internals
    private void record(TxnState.Builder st, TxnState.Part part) {
        String role = part.getRole();
        boolean replaced = false;
        for (int i = 0; i < st.getLatestCount(); i++) {
            if (st.getLatest(i).getRole().equals(role)) {
                st.setLatest(i, part);
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            st.addLatest(part);
            st.addFirst(part);
        }
        st.putCounts(role, st.getCountsOrDefault(role, 0) + 1);
        st.setArrived(st.getArrived() | contract.bit(role));
    }

    private List<Out> assemble(TxnState.Builder st) {
        st.setAssembled(true);
        st.setStage("assembled");
        List<Out> out = new ArrayList<>();
        TransactionEvent ev = unified(st, contract.fields, null);
        byte[] trace = st.getTriggerTrace().isEmpty() ? null : st.getTriggerTrace().toByteArray();
        out.add(new Out("assembled", ev, context(st), false, trace, st.getTriggerIngestUs(), st.getBackfill()));
        List<TxnState.Part> held = new ArrayList<>(st.getHeldList());
        st.clearHeld();
        for (TxnState.Part p : held) {
            record(st, p.toBuilder().clearTrace().build());
            out.addAll(stage(st, p.getRole(), p.getPayload().toByteArray(),
                    p.getTrace().isEmpty() ? null : p.getTrace().toByteArray(), p.getIngestUs()));
        }
        return out;
    }

    /** A lifecycle message: moves the stage; a scored stage is decided again, timed from its own message. */
    private List<Out> stage(TxnState.Builder st, String role, byte[] payload, byte[] trace, long ingestUs) {
        List<Out> out = new ArrayList<>();
        for (TransactionContract.Stage s : contract.stages) {
            if (!s.role().equals(role)) continue;
            if (s.when() != null) {
                try {
                    if (!matcher.matches(s.when(), codecs.get(role).view(payload))) continue;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            st.setStage(s.name());
            if (s.scored()) {
                TransactionEvent base = unified(st, contract.fields, null);
                TransactionEvent ev = unified(st, s.fields(), base);
                out.add(new Out("stage", ev, context(st), true, trace, ingestUs, st.getBackfill()));
            }
            break;
        }
        return out;
    }

    private Object[] values(TxnState.Builder st, String role, boolean first) {
        for (TxnState.Part p : first ? st.getFirstList() : st.getLatestList()) {
            if (p.getRole().equals(role)) {
                try {
                    return codecs.get(role).decode(p.getPayload().toByteArray());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return null;
    }

    private Long partTime(TxnState.Builder st, String role, boolean first) {
        for (TxnState.Part p : first ? st.getFirstList() : st.getLatestList()) {
            if (p.getRole().equals(role)) return p.getEventTimeUs();
        }
        return null;
    }

    /** The unified event from the parts: proto3 defaults, then each mapping (a part not arrived leaves it). */
    private TransactionEvent unified(TxnState.Builder st, List<TransactionContract.FieldMap> maps, TransactionEvent base) {
        TransactionEvent.Builder b = base != null ? base.toBuilder() : TransactionEvent.newBuilder();
        Map<String, Object[]> cache = new HashMap<>();
        for (TransactionContract.FieldMap fm : maps) {
            FieldDescriptor fd = into.findFieldByName(fm.target());
            if (fd == null) throw new IllegalArgumentException(contract.name + ": " + fm.target() + " is not a field of the unified event");
            if (fm.role() == null) {
                set(b, fd, constant(fd, fm.constant()));
                continue;
            }
            Object[] v = cache.computeIfAbsent(fm.role(), r -> values(st, r, false));
            if (v == null) continue;
            WireCodec c = codecs.get(fm.role());
            int i = c.indexOf(fm.field());
            if (i < 0) continue;
            Object x = v[i];
            FieldDescriptor src = c.descriptor.findFieldByName(fm.field());
            if (src.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                String name = (String) x;
                if (!fm.enumMap().isEmpty()) {
                    String target = fm.enumMap().get(name);
                    if (target == null) continue;   // unmapped: the default
                    set(b, fd, fd.getEnumType().findValueByName(target));
                } else if (fd.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    var ev = src.getEnumType().findValueByName(name);
                    int num = ev != null ? ev.getNumber() : Integer.parseInt(name);
                    set(b, fd, fd.getEnumType().findValueByNumberCreatingIfUnknown(num));
                } else {
                    var ev = src.getEnumType().findValueByName(name);
                    set(b, fd, (long) (ev != null ? ev.getNumber() : Integer.parseInt(name)));
                }
                continue;
            }
            set(b, fd, x);
        }
        return b.build();
    }

    private static Object constant(FieldDescriptor fd, JsonNode c) {
        return switch (fd.getJavaType()) {
            case ENUM -> fd.getEnumType().findValueByName(c.asText());
            case BOOLEAN -> c.asBoolean();
            case DOUBLE, FLOAT -> c.asDouble();
            case STRING -> c.asText();
            default -> c.asLong();
        };
    }

    /** Set a field from a decoded or constant value, converting to the field's Java type. */
    private static void set(Message.Builder b, FieldDescriptor fd, Object v) {
        if (v == null) return;
        Object x = switch (fd.getJavaType()) {
            case INT -> ((Number) v).intValue();
            case LONG -> ((Number) v).longValue();
            case BOOLEAN -> v instanceof Boolean bo ? bo : ((Number) v).longValue() != 0;
            case DOUBLE -> ((Number) v).doubleValue();
            case FLOAT -> ((Number) v).floatValue();
            case STRING -> v.toString();
            case ENUM -> v instanceof Descriptors.EnumValueDescriptor ? v
                    : fd.getEnumType().findValueByNumberCreatingIfUnknown(((Number) v).intValue());
            default -> throw new IllegalArgumentException("unsupported unified field " + fd.getName());
        };
        b.setField(fd, x);
    }

    /** The transaction's context now: its values over the parts received so far (TxnContext). */
    public TxnContext context(TxnState.Builder st) {
        TxnContext.Builder c = TxnContext.newBuilder().setTransaction(contract.name).setTxnId(st.getTxnId())
                .setStage(st.getStage()).setPartsPresent(st.getArrived()).setPartial(st.getPartial());
        for (TransactionContract.Context spec : contract.context) {
            switch (spec.kind()) {
                case "from" -> {
                    Object[] v = values(st, spec.role(), false);
                    if (v == null) continue;     // absent: the reader's default
                    Object x = v[codecs.get(spec.role()).indexOf(spec.field())];
                    if (x instanceof String s) c.putStrings(spec.name(), s);
                    else if (x instanceof Boolean bo) c.putNumbers(spec.name(), bo ? 1 : 0);
                    else c.putNumbers(spec.name(), ((Number) x).doubleValue());
                }
                case "count" -> c.putNumbers(spec.name(), st.getCountsOrDefault(spec.role(), 0));
                case "changed" -> {
                    Object[] a = values(st, spec.betweenA(), true);
                    Object[] b = values(st, spec.betweenB(), false);
                    boolean changed = false;
                    if (a != null && b != null) {
                        int ia = codecs.get(spec.betweenA()).indexOf(spec.field());
                        int ib = codecs.get(spec.betweenB()).indexOf(spec.field());
                        changed = ia >= 0 && ib >= 0 && !Objects.equals(a[ia], b[ib]);
                    }
                    c.putNumbers(spec.name(), changed ? 1 : 0);
                }
                case "elapsed" -> {
                    Long a = partTime(st, spec.betweenA(), true);
                    Long b = partTime(st, spec.betweenB(), false);
                    c.putNumbers(spec.name(), a != null && b != null ? (b - a) / 1e6 : 0.0);
                }
                default -> throw new IllegalStateException(spec.kind());
            }
        }
        return c.build();
    }
}
