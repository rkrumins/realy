package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.EntityRegistry;
import io.realy.scoring.binding.PipelineBinding;
import io.realy.scoring.binding.WireCodec;
import io.realy.scoring.features.EventRow;
import io.realy.scoring.features.EventView;
import io.realy.scoring.features.FeatureSchema;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Bindings and the generic codec: on the payment event they reproduce EventView and the payment row. */
class BindingTest {
    @Test
    void registryOrderOwnershipAndExactness() {
        EntityRegistry reg = EntityRegistry.load();
        assertThat(reg.names().subList(0, 7)).containsExactly("customer", "card", "device", "ip", "merchant", "terminal", "payee");
        assertThat(reg.owners("card")).containsExactly("customer");
        assertThat(reg.exact("customer")).containsExactly("customer", "card", "account", "txn");
        // the keyed-stage order is the registry's
        List<String> stages = List.copyOf(FeatureSchema.load().streamByEntity().keySet());
        assertThat(reg.names().stream().filter(stages::contains).toList()).isEqualTo(stages);
    }

    @Test
    void paymentsPipeline() {
        PipelineBinding p = PipelineBinding.current();
        assertThat(p.name).isEqualTo("payments");
        assertThat(p.scored("payment")).isTrue();
        assertThat(p.keyPrefix()).isEmpty();
        assertThat(p.binding("payment").message).isEqualTo("realy.payments.v1.TransactionEvent");
    }

    @Test
    void wireCodecEqualsEventViewAndRowsOnTheGoldenEvents() throws Exception {
        var events = GoldenPaymentsTest.events(GoldenPaymentsTest.dir());
        WireCodec c = WireCodec.of("realy.payments.v1.TransactionEvent");
        var b = PipelineBinding.current().binding("payment");
        var plan = FeatureSchema.load().rowPlan();
        for (TransactionEvent e : events) {
            Object[] v = c.decode(e.toByteArray());
            var legacy = new java.util.HashMap<>(EventView.of(e));
            assertThat(legacy.remove("txn")).isEqualTo(io.realy.scoring.binding.TxnContexts.defaults());
            assertThat(c.view(v)).as("event " + e.getEventId()).isEqualTo(legacy);
            EventRow g = plan.row(b, c, v);
            EventRow r = plan.row(e);
            assertThat(g.eventId).isEqualTo(r.eventId);
            assertThat(g.timeUs).isEqualTo(r.timeUs);
            assertThat(g.lat).isEqualTo(r.lat);
            assertThat(g.lon).isEqualTo(r.lon);
            assertThat(g.keys).isEqualTo(r.keys);
            assertThat(g.num).isEqualTo(r.num);
            assertThat(g.val).isEqualTo(r.val);
        }
    }

    @Test
    void unknownEnumNumbersAndAbsentFields() throws Exception {
        WireCodec c = WireCodec.of("realy.payments.v1.TransactionEvent");
        byte[] raw = TransactionEvent.newBuilder().setEventId(5).setPaymentTypeValue(42).build().toByteArray();
        var view = c.view(raw);
        assertThat(view.get("payment_type")).isEqualTo("42");
        assertThat(EventView.of(TransactionEvent.parseFrom(raw)).get("payment_type")).isEqualTo("42");
        assertThat(((java.util.Map<?, ?>) EventView.of(TransactionEvent.parseFrom(raw)).get("txn")).get("payee_check"))
                .isEqualTo("PAYEE_CHECK_UNSPECIFIED");
        assertThat(view.get("three_ds")).isEqualTo("THREE_DS_NONE");
        assertThat(view.get("merchant_country")).isEqualTo("");
        assertThat(view.get("lat")).isEqualTo(0.0d);
    }
}
