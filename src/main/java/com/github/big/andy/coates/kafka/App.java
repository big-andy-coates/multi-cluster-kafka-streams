package com.github.big.andy.coates.kafka;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public final class App implements AutoCloseable {

    private final KafkaStreams streams;

    public App(
            final String applicationId,
            final String sourceClusterBootstrap,
            final String destinationClusterBootstrap,
            final String sourceTopic,
            final String sinkTopic,
            final Map<String, ?> configOverrides) {
        final Topology topology = new TopologyBuilder().build(sourceTopic, sinkTopic);

        final KafkaClientSupplier clientSupplier =
                new MultiClusterKafkaClientSupplier(
                        sourceClusterBootstrap, destinationClusterBootstrap);

        this.streams =
                new KafkaStreams(
                        topology,
                        properties(
                                applicationId,
                                sourceClusterBootstrap,
                                destinationClusterBootstrap,
                                configOverrides),
                        clientSupplier);
    }

    public void start() {
        streams.start();

        while (streams.state() == KafkaStreams.State.REBALANCING) {
            Thread.yield();
        }

        if (streams.state() != KafkaStreams.State.RUNNING) {
            throw new RuntimeException("Failed to start streams app: " + streams.state());
        }
    }

    public void close() {
        streams.close();
    }

    private static Properties properties(
            final String applicationId,
            final String sourceClusterBootstrap,
            final String destinationClusterBootstrap,
            final Map<String, ?> configOverrides) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "not-used");
        props.putAll(configOverrides);
        return props;
    }
}
