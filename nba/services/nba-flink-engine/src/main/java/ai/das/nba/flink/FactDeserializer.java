package ai.das.nba.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/** Reads any NBA topic into {@link FactRecord} — key + value + the "kind" header + the kafka timestamp. */
public class FactDeserializer implements KafkaRecordDeserializationSchema<FactRecord> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> rec, Collector<FactRecord> out) {
        String key = rec.key() == null ? null : new String(rec.key(), StandardCharsets.UTF_8);
        String value = rec.value() == null ? null : new String(rec.value(), StandardCharsets.UTF_8);
        String kind = null;
        Header h = rec.headers().lastHeader("kind");
        if (h != null && h.value() != null) kind = new String(h.value(), StandardCharsets.UTF_8);
        if (value == null) return;   // tombstone — skip
        out.collect(new FactRecord(key, value, kind, rec.timestamp()));
    }

    @Override
    public TypeInformation<FactRecord> getProducedType() { return TypeInformation.of(FactRecord.class); }
}
