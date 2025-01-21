package com.github.big.andy.coates.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public final class TopologyBuilder {

    public Topology build(final String sourceTopic, final String sinkTopic) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(
                        sourceTopic,
                        Consumed.with(Serdes.Long(), Serdes.Integer()).withName("source"))
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.Integer()).withName("group-by-key"))
                .reduce(
                        Integer::sum,
                        Named.as("reduce"),
                        Materialized.<Long, Integer, KeyValueStore<Bytes, byte[]>>as(
                                        "reduced-store")
                                .withKeySerde(Serdes.Long())
                                .withValueSerde(Serdes.Integer()))
                .toStream(Named.as("to-stream"))
                .to(sinkTopic, Produced.with(Serdes.Long(), Serdes.Integer()).withName("sink"));

        return builder.build();
    }
}
