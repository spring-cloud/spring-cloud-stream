/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for materializing an incoming KTable as a versioned state store.
 *
 * @author Oleg Zhurakousky
 */
@EmbeddedKafka
class KafkaStreamsVersionedKTableTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	void incomingKTableIsMaterializedAsVersionedStore() throws Exception {
		String suffix = UUID.randomUUID().toString().substring(0, 8);
		String appId = "versioned-" + suffix;
		try (ConfigurableApplicationContext context = run(suffix, appId,
				"--spring.cloud.stream.kafka.streams.bindings.process-in-1.consumer.materializedAs=versioned-store",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-1.consumer.versionedHistoryRetention=PT1H")) {
			send("table-" + suffix, "alice", "asia");
			send("stream-" + suffix, "alice", "click");
			assertThat(VersionedApplication.latch.await(20, TimeUnit.SECONDS)).isTrue();

			KafkaStreams streams = context.getBean(StreamsBuilderFactoryBean.class).getKafkaStreams();
			QueryResult<VersionedRecord<String>> result = versionedQuery(streams, "versioned-store");
			assertThat(result.isSuccess()).isTrue();
			assertThat(result.getResult().value()).isEqualTo("asia");

			// With topology.optimization=all the source topic is reused, so no changelog is created.
			assertThat(topics()).noneMatch((t) -> t.contains(appId) && t.endsWith("-changelog"));
		}
	}

	@Test
	void incomingKTableIsMaterializedAsKeyValueStoreByDefault() throws Exception {
		String suffix = UUID.randomUUID().toString().substring(0, 8);
		try (ConfigurableApplicationContext context = run(suffix, "plain-" + suffix,
				"--spring.cloud.stream.kafka.streams.bindings.process-in-1.consumer.materializedAs=plain-store")) {
			send("table-" + suffix, "alice", "asia");
			send("stream-" + suffix, "alice", "click");
			assertThat(VersionedApplication.latch.await(20, TimeUnit.SECONDS)).isTrue();

			KafkaStreams streams = context.getBean(StreamsBuilderFactoryBean.class).getKafkaStreams();
			ReadOnlyKeyValueStore<String, Object> store = streams.store(
					StoreQueryParameters.fromNameAndType("plain-store", QueryableStoreTypes.keyValueStore()));
			assertThat(store.get("alice")).isNotNull();
			// A plain KeyValueStore does not answer versioned queries.
			assertThat(versionedQuery(streams, "plain-store").isFailure()).isTrue();
		}
	}

	@Test
	void versionedHistoryRetentionRequiresMaterializedAs() {
		String suffix = UUID.randomUUID().toString().substring(0, 8);
		assertThatThrownBy(() -> run(suffix, "invalid-" + suffix,
				"--spring.cloud.stream.kafka.streams.bindings.process-in-1.consumer.versionedHistoryRetention=PT1H"))
				.hasStackTraceContaining("requires 'materializedAs'");
	}

	@SuppressWarnings("unchecked")
	private QueryResult<VersionedRecord<String>> versionedQuery(KafkaStreams streams, String store) {
		StateQueryRequest<VersionedRecord<String>> request = StateQueryRequest.inStore(store)
				.withQuery(VersionedKeyQuery.<String, String>withKey("alice"))
				.withAllPartitions();
		StateQueryResult<VersionedRecord<String>> result = streams.query(request);
		return result.getOnlyPartitionResult() != null
				? result.getOnlyPartitionResult()
				: (QueryResult<VersionedRecord<String>>) result.getPartitionResults().values().iterator().next();
	}

	private ConfigurableApplicationContext run(String suffix, String appId, String... extra) {
		VersionedApplication.latch = new CountDownLatch(1);
		SpringApplication app = new SpringApplication(VersionedApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		String[] common = {"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=stream-" + suffix,
				"--spring.cloud.stream.bindings.process-in-1.destination=table-" + suffix,
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.topology.optimization=all",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=100",
				"--spring.cloud.stream.kafka.streams.binder.applicationId=" + appId,
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString()};
		String[] all = new String[common.length + extra.length];
		System.arraycopy(common, 0, all, 0, common.length);
		System.arraycopy(extra, 0, all, common.length, extra.length);
		return app.run(all);
	}

	private void send(String topic, String key, String value) {
		Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
		try {
			KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic(topic);
			template.sendDefault(key, value);
		}
		finally {
			pf.destroy();
		}
	}

	private Set<String> topics() throws Exception {
		try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", embeddedKafka.getBrokersAsString()))) {
			return admin.listTopics().names().get(10, TimeUnit.SECONDS);
		}
	}

	@EnableAutoConfiguration
	public static class VersionedApplication {

		static CountDownLatch latch;

		@Bean
		public BiConsumer<KStream<String, String>, KTable<String, String>> process() {
			return (input, table) -> input.join(table, (v1, v2) -> v1 + ":" + v2)
					.foreach((k, v) -> latch.countDown());
		}

	}

}
