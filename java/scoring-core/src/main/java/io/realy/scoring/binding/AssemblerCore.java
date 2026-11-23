package io.realy.scoring.binding;

import io.realy.contracts.flows.v1.TxnState;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The assembly logic of one partition key (a customer), over a store of its open transactions: the Flink
 * operator backs the store with keyed MapState, tests with a map. Routes each message to the transaction
 * contracts it is part of, closes overdue waits first (a message of the same key later than a trigger's
 * event time + wait), and passes everything else (the unified event) through.
 */
public final class AssemblerCore {
    /** The open transactions of one key, by "&lt;contract&gt;:&lt;link id&gt;". */
    public interface Store {
        Iterable<String> keys() throws Exception;

        TxnState get(String key) throws Exception;

        void put(String key, TxnState state) throws Exception;

        void remove(String key) throws Exception;
    }

    private record Route(Assembly assembly, List<String> roles) {}

    private final Map<String, Assembly> byName = new HashMap<>();
    private final Map<String, List<Route>> byType = new HashMap<>();

    public AssemblerCore(List<String> contracts, Assembly.StageMatcher matcher) {
        for (String n : contracts) {
            Assembly a = new Assembly(TransactionContract.load(n), matcher);
            byName.put(n, a);
            for (String type : new java.util.LinkedHashSet<>(a.contract.roles.values())) {
                byType.computeIfAbsent(type, k -> new ArrayList<>()).add(new Route(a, a.contract.rolesOf(type)));
            }
        }
    }

    /** Whether messages of this event type are parts of a transaction (else: pass through). */
    public boolean isPart(String type) {
        return byType.containsKey(type);
    }

    public Iterable<Assembly> assemblies() {
        return byName.values();
    }

    /**
     * One message of the key, in partition order. Returns the events it releases (overdue transactions
     * first); a message that is not a part returns them and the caller passes it through after them.
     */
    public List<Assembly.Out> process(Store store, String type, byte[] payload, long eventTimeUs, byte[] trace,
                                      long ingestUs, long nowWallUs, boolean backfill) throws Exception {
        List<Assembly.Out> out = new ArrayList<>(closeOverdue(store, eventTimeUs));
        List<Route> routes = byType.get(type);
        if (routes == null) return out;
        for (Route rt : routes) {
            Assembly a = rt.assembly();
            long txnId = linkId(a, rt.roles().get(0), payload);
            String key = a.contract.name + ":" + Long.toUnsignedString(txnId);
            TxnState have = store.get(key);
            TxnState.Builder st = have != null ? have.toBuilder() : TxnState.newBuilder().setTxnId(txnId);
            for (String role : rt.roles()) {
                out.addAll(a.onPart(st, role, payload, eventTimeUs, trace, ingestUs, nowWallUs, backfill));
            }
            if (a.finished(st.build())) store.remove(key);
            else store.put(key, st.build());
        }
        return out;
    }

    /** Waits that a later message of the key closes (event time beyond trigger + wait). */
    public List<Assembly.Out> closeOverdue(Store store, long eventTimeUs) throws Exception {
        List<Assembly.Out> out = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (String k : store.keys()) keys.add(k);
        for (String k : keys) {
            TxnState s = store.get(k);
            Assembly a = byName.get(s.getTransaction());
            if (a != null && a.overdue(s, eventTimeUs)) {
                TxnState.Builder b = s.toBuilder();
                out.addAll(a.onDeadline(b));
                store.put(k, b.build());
            }
        }
        return out;
    }

    /** The wall-clock wait expired (the operator's timer): assemble what is due without its missing parts. */
    public List<Assembly.Out> onTimer(Store store, long nowWallUs) throws Exception {
        List<Assembly.Out> out = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (String k : store.keys()) keys.add(k);
        for (String k : keys) {
            TxnState s = store.get(k);
            Assembly a = byName.get(s.getTransaction());
            if (a != null && !s.getAssembled() && s.getTriggerTimeUs() != 0 && s.getDeadlineWallUs() <= nowWallUs) {
                TxnState.Builder b = s.toBuilder();
                out.addAll(a.onDeadline(b));
                store.put(k, b.build());
            }
        }
        return out;
    }

    /** Assemble everything still waiting (end of a bounded stream). */
    public List<Assembly.Out> closeAll(Store store) throws Exception {
        return closeOverdue(store, Long.MAX_VALUE);
    }

    private static long linkId(Assembly a, String role, byte[] payload) {
        WireCodec c = a.codec(role);
        try {
            Object v = c.decode(payload)[c.indexOf(a.contract.link)];
            return ((Number) v).longValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
