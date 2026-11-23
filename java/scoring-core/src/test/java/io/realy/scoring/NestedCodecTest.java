package io.realy.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Timestamp;
import io.realy.contracts.payments.v1.AccountInfo;
import io.realy.contracts.payments.v1.Address;
import io.realy.contracts.payments.v1.Authentication;
import io.realy.contracts.payments.v1.CardInfo;
import io.realy.contracts.payments.v1.CardStatus;
import io.realy.contracts.payments.v1.LineItem;
import io.realy.contracts.payments.v1.MerchantInfo;
import io.realy.contracts.payments.v1.ThreeDsResult;
import io.realy.contracts.payments.v1.TransactionEvent;
import io.realy.scoring.binding.WireCodec;
import io.realy.scoring.rules.Cel;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Nested messages, repeated and map fields, and well-known types through the generic codec and CEL. */
class NestedCodecTest {
    static final WireCodec TE = WireCodec.of(TransactionEvent.getDescriptor());

    static TransactionEvent rich() {
        return TransactionEvent.newBuilder().setEventId(7).setEventTimeUs(1_000).setCustomerId(3).setCardId(11)
                .setCardInfo(CardInfo.newBuilder().setId(11).setBin("411111").setBinCountry("US")
                        .setStatus(CardStatus.CARD_STATUS_BLOCKED).setTokenized(true).setWallet("APPLE_PAY"))
                .setAccountInfo(AccountInfo.newBuilder().setId(5).setBalanceUsdMinor(-1200).addFlags("AML_HOLD").addFlags("OVERDRAWN"))
                .setMerchantInfo(MerchantInfo.newBuilder().setId(9).setMcc(5411).setRiskTier(3)
                        .setAddress(Address.newBuilder().setCountry("GB").setCity("London")))
                .setAuthentication(Authentication.newBuilder().setThreeDs(ThreeDsResult.THREE_DS_AUTHENTICATED).setAttempts(2))
                .addItems(LineItem.newBuilder().setSku("A").setQty(1).setAmountUsdMinor(500))
                .addItems(LineItem.newBuilder().setSku("B").setQty(3).setAmountUsdMinor(900))
                .putAttributes("channel_app", "ios")
                .build();
    }

    @Test
    void nestedScalarsHaveSlotsByDottedPath() throws Exception {
        Object[] v = TE.decode(rich().toByteArray());
        assertThat(v[TE.indexOf("card_info.status")]).isEqualTo("CARD_STATUS_BLOCKED");
        assertThat(v[TE.indexOf("card_info.id")]).isEqualTo(11L);
        assertThat(v[TE.indexOf("merchant_info.address.country")]).isEqualTo("GB");
        assertThat(v[TE.indexOf("authentication.attempts")]).isEqualTo(2L);
        assertThat(v[TE.indexOf("account_info.balance_usd_minor")]).isEqualTo(-1200L);
        assertThat(v[TE.indexOf("account_info.flags")]).isEqualTo(List.of("AML_HOLD", "OVERDRAWN"));
        assertThat(v[TE.indexOf("attributes")]).isEqualTo(Map.of("channel_app", "ios"));
        @SuppressWarnings("unchecked") List<Map<String, Object>> items = (List<Map<String, Object>>) v[TE.indexOf("items")];
        assertThat(items).hasSize(2);
        assertThat(items.get(1).get("qty")).isEqualTo(3L);
        // numeric reads: an enum as its number, a bool as 1 / 0
        assertThat(TE.numeric(TE.indexOf("card_info.status"), v[TE.indexOf("card_info.status")])).isEqualTo(2.0);
        assertThat(TE.numeric(TE.indexOf("card_info.tokenized"), v[TE.indexOf("card_info.tokenized")])).isEqualTo(1.0);
        // flat fields are where they were
        assertThat(v[TE.indexOf("card_id")]).isEqualTo(11L);
    }

    @Test
    void absentBlocksReadTheirDefaults() throws Exception {
        TransactionEvent flat = TransactionEvent.newBuilder().setEventId(1).setCardId(4).build();
        Object[] v = TE.decode(flat.toByteArray());
        assertThat(v[TE.indexOf("card_info.status")]).isEqualTo("CARD_STATUS_UNSPECIFIED");
        assertThat(v[TE.indexOf("card_info.id")]).isEqualTo(0L);
        assertThat(v[TE.indexOf("items")]).isEqualTo(List.of());
        @SuppressWarnings("unchecked") Map<String, Object> card = (Map<String, Object>) TE.view(v).get("card_info");
        assertThat(card.get("bin")).isEqualTo("");
        assertThat(flat.getSerializedSize()).isEqualTo(TransactionEvent.newBuilder().setEventId(1).setCardId(4).build().getSerializedSize());
    }

    @Test
    void rulesReadNestedMapsListsAndMaps() throws Exception {
        Map<String, Object> event = TE.view(rich().toByteArray());
        Map<String, Object> vars = Map.of("event", event, "f", Map.of());
        assertThat(Cel.compile("event.card_info.status == 'CARD_STATUS_BLOCKED'").test(vars)).isTrue();
        assertThat(Cel.compile("event.merchant_info.address.country == 'GB' && event.merchant_info.risk_tier >= 3").test(vars)).isTrue();
        assertThat(Cel.compile("size(event.items) == 2 && event.items.exists(i, i.qty > 2)").test(vars)).isTrue();
        assertThat(Cel.compile("'AML_HOLD' in event.account_info.flags").test(vars)).isTrue();
        assertThat(Cel.compile("'channel_app' in event.attributes && event.attributes['channel_app'] == 'ios'").test(vars)).isTrue();
        assertThat(Cel.compile("event.card_info.wallet != '' && event.authentication.attempts > 1").test(vars)).isTrue();
        Map<String, Object> flat = TE.view(TransactionEvent.newBuilder().setEventId(1).build().toByteArray());
        assertThat(Cel.compile("event.card_info.status == 'CARD_STATUS_BLOCKED' || size(event.items) > 0")
                .test(Map.of("event", flat, "f", Map.of()))).isFalse();
    }

    @Test
    void thePaymentFastPathReadsBlocksAsTheGenericCodecDoes() throws Exception {
        for (TransactionEvent e : List.of(rich(), TransactionEvent.newBuilder().setEventId(1).setCardId(4).build())) {
            Map<String, Object> fast = io.realy.scoring.features.EventView.of(e);
            Map<String, Object> generic = TE.view(e.toByteArray());
            for (String block : List.of("card_info", "account_info", "merchant_info", "device_info", "payee_info",
                    "authentication", "items", "attributes")) {
                assertThat(fast.get(block)).as(block).isEqualTo(generic.get(block));
            }
        }
        TransactionEvent e = rich();
        assertThat(io.realy.scoring.features.EventView.numeric(e, "card_info.status")).isEqualTo(2.0);   // enum number
        assertThat(io.realy.scoring.features.EventView.numeric(e, "authentication.attempts")).isEqualTo(2.0);
        assertThat(io.realy.scoring.features.EventView.numeric(e, "card_info.tokenized")).isEqualTo(1.0);
        assertThat(io.realy.scoring.features.EventView.valueKey(e, "card_info.bin"))
                .isEqualTo(io.realy.common.Hashing.fnv1a64("411111"));
        assertThat(io.realy.scoring.features.EventView.valueKey(e, "merchant_info.address.country"))
                .isEqualTo(io.realy.common.Hashing.fnv1a64("GB"));
        // an absent block reads its defaults, the flat path is untouched
        TransactionEvent flat = TransactionEvent.newBuilder().setCardId(4).build();
        assertThat(io.realy.scoring.features.EventView.numeric(flat, "card_info.status")).isEqualTo(0.0);
        assertThat(io.realy.scoring.features.EventView.entityKey(flat, "card")).isEqualTo(4L);
    }

    /** A message built here: packed and unpacked repeated ints, a Timestamp, a wrapper, a map of ints. */
    @Test
    void packedUnpackedTimestampsWrappersAndMaps() throws Exception {
        FileDescriptorProto fp = FileDescriptorProto.newBuilder().setName("t.proto").setPackage("t").setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto").addDependency("google/protobuf/wrappers.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("M")
                        .addField(field("packed", 1, FieldDescriptorProto.Type.TYPE_INT64, true).build())
                        .addField(field("unpacked", 2, FieldDescriptorProto.Type.TYPE_INT64, true)
                                .setOptions(FieldOptions.newBuilder().setPacked(false)).build())
                        .addField(FieldDescriptorProto.newBuilder().setName("at").setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Timestamp"))
                        .addField(FieldDescriptorProto.newBuilder().setName("limit").setNumber(4)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Int64Value")))
                .build();
        Descriptors.FileDescriptor f = Descriptors.FileDescriptor.buildFrom(fp, new Descriptors.FileDescriptor[] {
                Timestamp.getDescriptor().getFile(), Int64Value.getDescriptor().getFile()});
        Descriptors.Descriptor m = f.findMessageTypeByName("M");
        DynamicMessage msg = DynamicMessage.newBuilder(m)
                .addRepeatedField(m.findFieldByName("packed"), 1L).addRepeatedField(m.findFieldByName("packed"), 2L)
                .addRepeatedField(m.findFieldByName("unpacked"), 3L).addRepeatedField(m.findFieldByName("unpacked"), 4L)
                .setField(m.findFieldByName("at"), Timestamp.newBuilder().setSeconds(10).setNanos(5_000).build())
                .setField(m.findFieldByName("limit"), Int64Value.of(250))
                .build();
        WireCodec c = WireCodec.of(m);
        Object[] v = c.decode(msg.toByteArray());
        assertThat(v[c.indexOf("packed")]).isEqualTo(List.of(1L, 2L));
        assertThat(v[c.indexOf("unpacked")]).isEqualTo(List.of(3L, 4L));
        assertThat(v[c.indexOf("at")]).isEqualTo(10_000_005L);   // microseconds
        assertThat(v[c.indexOf("limit")]).isEqualTo(250L);        // unwrapped
    }

    private static FieldDescriptorProto.Builder field(String name, int number, FieldDescriptorProto.Type type, boolean repeated) {
        FieldDescriptorProto.Builder b = FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type);
        if (repeated) b.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
        return b;
    }
}
