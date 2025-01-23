package com.github.big.andy.coates.kafka;

import static org.apache.kafka.streams.KeyValue.pair;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
public class MultiClusterKafkaFunctionalTest {

    private static final Map<String, String> KAFKA_CLUSTER_ENV =
            Map.of(
                    // `group.initial.rebalance.delay.ms` reduced to speed up test.
                    // Commenting this out does _not_ allow the consumer to rejoin quickly
                    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0",
                    "KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000",
                    "KAFKA_DEFAULT_REPLICATION_FACTOR", "1",
                    "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");

    @Container
    private static final ConfluentKafkaContainer SOURCE_CLUSTER =
            new ConfluentKafkaContainer(
                            "confluentinc/cp-kafka:%s"
                                    .formatted(System.getProperty("confluentVersion")))
                    .withEnv(KAFKA_CLUSTER_ENV);

    @Container
    private static final ConfluentKafkaContainer DESTINATION_CLUSTER =
            new ConfluentKafkaContainer(
                            "confluentinc/cp-kafka:%s"
                                    .formatted(System.getProperty("confluentVersion")))
                    .withEnv(KAFKA_CLUSTER_ENV);

    private static final Map<String, ?> TEST_APP_CONFIG =
            Map.of(
                    // Configure things to process & rebalance more quickly in tests:
                    StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1,
                    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100,
                    ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 250
                    // Uncommenting this will make
                    // shouldRestoreStateStoreFromChangeLogInDestinationCluster() run in seconds,
                    // not 45+ seconds:
                    // , ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1_000
                    );

    private String applicationId;
    private String sourceTopic;
    private String sinkTopic;
    private String changelogTopic;
    private String consumerGroupId;
    private Consumer<Long, Integer> consumer;
    private App app;

    @BeforeEach
    void setUp() {
        applicationId = randomName();
        sourceTopic = "source-" + applicationId;
        sinkTopic = "sink-" + applicationId;
        changelogTopic = applicationId + "-reduced-store-changelog";

        provisionTopic(SOURCE_CLUSTER, sourceTopic);
        provisionTopic(DESTINATION_CLUSTER, sinkTopic);

        consumerGroupId = UUID.randomUUID().toString();
        consumer = createConsumer();
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.close();
        }

        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shouldPopulateChangeLogTopicOnDestinationCluster() {
        // Given:
        produceTo(sourceTopic, 5L, 1);

        // When:
        createApp();

        // Then:
        final KeyValue<Long, Integer> record = consumeFrom(changelogTopic);
        assertThat(record, is(pair(5L, 1)));
    }

    @Test
    void shouldPopulateSinkTopicOnDestinationCluster() {
        // Given:
        produceTo(sourceTopic, 4L, 1);

        // When:
        createApp();

        // Then:
        final KeyValue<Long, Integer> record = consumeFrom(sinkTopic);
        assertThat(record, is(pair(4L, 1)));
    }

    @Test
    void shouldStoreConsumerOffsetsInSourceCluster() {
        // Given:
        produceTo(sourceTopic, 3L, 5);

        // When:
        createApp();
        consumeFrom(sinkTopic);
        app.close();

        // Then:
        final Set<String> groups = sourceConsumerGroups();
        assertThat(groups, is(Set.of(applicationId)));
    }

    @Test
    void shouldRestoreStateStoreFromChangeLogInDestinationCluster() {
        // Given:
        createApp();
        app.close();

        produceTo(changelogTopic, 5L, 2);
        produceTo(sourceTopic, 5L, 3);

        // When:
        createApp(); // <-- Test pauses here for ~45 seconds

        // Then:
        final KeyValue<Long, Integer> record = consumeFrom(sinkTopic);
        assertThat(record, is(pair(5L, 5)));
    }

    private void createApp() {
        if (app != null) {
            app.close();
        }

        app =
                new App(
                        applicationId,
                        SOURCE_CLUSTER.getBootstrapServers(),
                        DESTINATION_CLUSTER.getBootstrapServers(),
                        sourceTopic,
                        sinkTopic,
                        TEST_APP_CONFIG);

        app.start();
    }

    private void produceTo(final String topic, final long key, final int value) {
        final ConfluentKafkaContainer cluster =
                topic.equals(sourceTopic) ? SOURCE_CLUSTER : DESTINATION_CLUSTER;

        final Map<String, Object> props =
                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());

        try (Producer<Long, Integer> producer =
                new KafkaProducer<>(props, new LongSerializer(), new IntegerSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value));
        }
    }

    private Consumer<Long, Integer> createConsumer() {
        final Map<String, Object> props =
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        DESTINATION_CLUSTER.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG,
                        consumerGroupId,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                        1,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        OffsetResetStrategy.EARLIEST.toString());

        return new KafkaConsumer<>(props, new LongDeserializer(), new IntegerDeserializer());
    }

    private KeyValue<Long, Integer> consumeFrom(final String topic) {
        consumer.subscribe(Set.of(topic));
        final ConsumerRecords<Long, Integer> records = consumer.poll(Duration.ofMinutes(1));
        assertThat(records.count(), is(1));
        final ConsumerRecord<Long, Integer> record = records.iterator().next();
        return pair(record.key(), record.value());
    }

    private static Set<String> sourceConsumerGroups() {
        try (Admin admin =
                AdminClient.create(
                        Map.of(
                                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                SOURCE_CLUSTER.getBootstrapServers()))) {

            return admin.listConsumerGroups().all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void provisionTopic(final ConfluentKafkaContainer cluster, final String topic) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());

        try (Admin admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        }
    }

    private static String randomName() {
        return UUID.randomUUID().toString().replaceAll("-", "_");
    }
}
