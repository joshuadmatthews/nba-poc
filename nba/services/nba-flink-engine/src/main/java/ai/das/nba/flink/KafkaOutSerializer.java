package ai.das.nba.flink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/** Serializes a {@link KafkaOut} to a fixed topic, carrying the key + value + an optional "kind" header. */
public class KafkaOutSerializer implements KafkaRecordSerializationSchema<KafkaOut> {
    private final String topic;

    public KafkaOutSerializer(String topic) { this.topic = topic; }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaOut e, KafkaSinkContext ctx, @Nullable Long ts) {
        byte[] k = e.key == null ? null : e.key.getBytes(StandardCharsets.UTF_8);
        byte[] v = e.value == null ? null : e.value.getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, k, v);
        if (e.kind != null) rec.headers().add(new RecordHeader("kind", e.kind.getBytes(StandardCharsets.UTF_8)));
        return rec;
    }
}
