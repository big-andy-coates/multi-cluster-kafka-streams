package com.github.big.andy.coates.kafka;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class MultiClusterKafkaClientSupplier extends DefaultKafkaClientSupplier {

    private final String sourceClusterBootstrap;
    private final String destinationClusterBootstrap;

    public MultiClusterKafkaClientSupplier(
            final String sourceClusterBootstrap, final String destinationClusterBootstrap) {
        this.sourceClusterBootstrap =
                requireNonNull(sourceClusterBootstrap, "sourceClusterBootstrap");
        this.destinationClusterBootstrap =
                requireNonNull(destinationClusterBootstrap, "destinationClusterBootstrap");
    }

    @Override
    public Admin getAdmin(final Map<String, Object> config) {
        // Admin client used to manage internal topics, which will be stored on the destination
        // cluster:
        return super.getAdmin(withBootstrap(config, destinationClusterBootstrap));
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        return super.getConsumer(withBootstrap(config, sourceClusterBootstrap));
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
        return super.getProducer(withBootstrap(config, destinationClusterBootstrap));
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
        // Used to restore state stores
        // Internal topics are written to destination cluster
        // So this consumer must also read from destination cluster
        return super.getRestoreConsumer(withBootstrap(config, destinationClusterBootstrap));
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
        // Example does not have global stores, so not customised.
        // Could be directed at either cluster, if needed.
        return super.getGlobalConsumer(config);
    }

    private static Map<String, Object> withBootstrap(
            final Map<String, Object> config, final String bootstrap) {
        final Map<String, Object> updated = new HashMap<>(config);
        updated.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return updated;
    }
}
