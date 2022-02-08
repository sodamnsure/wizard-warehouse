package com.wizard.warehouse.realtime.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * The deserialization schema describes how to turn the Kafka ConsumerRecords into data types
 * (Java/Scala objects) that are processed by Flink.
 */
public class UniqDeserializer implements KafkaDeserializationSchema<Tuple2<String, String>> {

    /**
     * Method to decide whether the element signals the end of the stream. If true is returned the
     * element won't be emitted.
     *
     * @return True, if the element signals end of stream, false otherwise.
     */
    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    /**
     * Deserializes the Kafka record.
     *
     * @param record Kafka record to be deserialized.
     * @return The deserialized message as a tuple2 that contains uniq id and data
     */
    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        // the topic this record is received from (never null)
        String topic = record.topic();
        // the partition from which this record is received
        int partition = record.partition();
        // the position of this record in the corresponding kafka partition.
        long offset = record.offset();
        // the string "topic-partition-offset" is treated as a unique id
        String uniqueId = topic + "-" + partition + "-" + offset;
        // constructs a new string by decoding the specified array of bytes using the specified charset.
        String value = new String(record.value(), StandardCharsets.UTF_8);
        // return a tuple that contains unique id and the record contents
        return Tuple2.of(uniqueId, value);
    }

    /**
     * Gets the data type (as a TypeInformation) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }
}
