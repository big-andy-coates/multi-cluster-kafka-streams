package com.github.big.andy.coates.kafka;

import java.util.Map;
import java.util.Properties;
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

        this.streams =
                new KafkaStreams(
                        topology,
                        properties(
                                applicationId,
                                configOverrides,
                                sourceClusterBootstrap,
                                destinationClusterBootstrap));
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
            final Map<String, ?> configOverrides,
            final String sourceClusterBootstrap,
            final String destinationClusterBootstrap) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        // Default to destination cluster for producing & restoring
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, destinationClusterBootstrap);
        // Override where main data is consumed from:
        props.put(
                StreamsConfig.MAIN_CONSUMER_PREFIX + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                sourceClusterBootstrap);
        // Currently, global stores are restored from destination cluster, to switch to source
        // uncomment:
        // props.put(StreamsConfig.GLOBAL_CONSUMER_PREFIX + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        // sourceClusterBootstrap);
        props.putAll(configOverrides);
        return props;
    }
}
