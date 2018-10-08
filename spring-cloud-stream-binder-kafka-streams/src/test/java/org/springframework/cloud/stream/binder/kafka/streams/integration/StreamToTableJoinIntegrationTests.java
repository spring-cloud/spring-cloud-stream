/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsApplicationSupportProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
public class StreamToTableJoinIntegrationTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, "output-topic");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<String, Long> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		DefaultKafkaConsumerFactory<String, Long> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "output-topic");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@EnableBinding(KafkaStreamsProcessorX.class)
	@EnableAutoConfiguration
	@EnableConfigurationProperties(KafkaStreamsApplicationSupportProperties.class)
	public static class CountClicksPerRegionApplication {

		@StreamListener
		@SendTo("output")
		public KStream<String, Long> process(@Input("input") KStream<String, Long> userClicksStream,
											@Input("input-x") KTable<String, String> userRegionsTable) {

			return userClicksStream
					.leftJoin(userRegionsTable, (clicks, region) -> new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks),
							Joined.with(Serdes.String(), Serdes.Long(), null))
					.map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(), regionWithClicks.getClicks()))
					.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
					.reduce((firstClicks, secondClicks) -> firstClicks + secondClicks)
					.toStream();
		}
	}

	interface KafkaStreamsProcessorX extends KafkaStreamsProcessor {

		@Input("input-x")
		KTable<?, ?> inputX();
	}

	@Test
	public void testStreamToTable() {
		SpringApplication app = new SpringApplication(CountClicksPerRegionApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext ignored = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=user-clicks",
				"--spring.cloud.stream.bindings.input-x.destination=user-regions",
				"--spring.cloud.stream.bindings.output.destination=output-topic",
				"--spring.cloud.stream.bindings.input.consumer.useNativeDecoding=true",
				"--spring.cloud.stream.bindings.input-x.consumer.useNativeDecoding=true",
				"--spring.cloud.stream.bindings.output.producer.useNativeEncoding=true",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.keySerde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.valueSerde=org.apache.kafka.common.serialization.Serdes$LongSerde",
				"--spring.cloud.stream.kafka.streams.bindings.inputX.consumer.keySerde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.inputX.consumer.valueSerde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.output.producer.keySerde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.output.producer.valueSerde=org.apache.kafka.common.serialization.Serdes$LongSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=10000",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId=StreamToTableJoinIntegrationTests-abc",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.kafka.streams.binder.zkNodes=" + embeddedKafka.getZookeeperConnectionString())) {
			// Input 1: Clicks per user (multiple records allowed per user).
			List<KeyValue<String, Long>> userClicks = Arrays.asList(
					new KeyValue<>("alice", 13L),
					new KeyValue<>("bob", 4L),
					new KeyValue<>("chao", 25L),
					new KeyValue<>("bob", 19L),
					new KeyValue<>("dave", 56L),
					new KeyValue<>("eve", 78L),
					new KeyValue<>("alice", 40L),
					new KeyValue<>("fang", 99L)
			);

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

			DefaultKafkaProducerFactory<String, Long> pf = new DefaultKafkaProducerFactory<>(senderProps);
			KafkaTemplate<String, Long> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("user-clicks");

			for (KeyValue<String, Long> keyValue : userClicks) {
				template.sendDefault(keyValue.key, keyValue.value);
			}

			// Input 2: Region per user (multiple records allowed per user).
			List<KeyValue<String, String>> userRegions = Arrays.asList(
					new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
					new KeyValue<>("bob", "americas"),
					new KeyValue<>("chao", "asia"),
					new KeyValue<>("dave", "europe"),
					new KeyValue<>("alice", "europe"), /* ...but moved to Europe some time later. */
					new KeyValue<>("eve", "americas"),
					new KeyValue<>("fang", "asia")
			);

			Map<String, Object> senderProps1 = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			senderProps1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

			DefaultKafkaProducerFactory<String, String> pf1 = new DefaultKafkaProducerFactory<>(senderProps1);
			KafkaTemplate<String, String> template1 = new KafkaTemplate<>(pf1, true);
			template1.setDefaultTopic("user-regions");

			for (KeyValue<String, String> keyValue : userRegions) {
				template1.sendDefault(keyValue.key, keyValue.value);
			}

			List<KeyValue<String, Long>> expectedClicksPerRegion = Arrays.asList(
					new KeyValue<>("americas", 101L),
					new KeyValue<>("europe", 109L),
					new KeyValue<>("asia", 124L)
			);

			//Verify that we receive the expected data
			int count = 0;
			long start = System.currentTimeMillis();
			List<KeyValue<String, Long>> actualClicksPerRegion = new ArrayList<>();
			do {
				ConsumerRecords<String, Long> records = KafkaTestUtils.getRecords(consumer);
				count = count + records.count();
				for (ConsumerRecord<String, Long> record : records) {
					actualClicksPerRegion.add(new KeyValue<>(record.key(), record.value()));
				}
			} while (count < expectedClicksPerRegion.size() && (System.currentTimeMillis() - start) < 30000);

			assertThat(count == expectedClicksPerRegion.size()).isTrue();
			assertThat(actualClicksPerRegion).hasSameElementsAs(expectedClicksPerRegion);
		}
	}

	/**
	 * Tuple for a region and its associated number of clicks.
	 */
	private static final class RegionWithClicks {

		private final String region;
		private final long clicks;

		RegionWithClicks(String region, long clicks) {
			if (region == null || region.isEmpty()) {
				throw new IllegalArgumentException("region must be set");
			}
			if (clicks < 0) {
				throw new IllegalArgumentException("clicks must not be negative");
			}
			this.region = region;
			this.clicks = clicks;
		}

		public String getRegion() {
			return region;
		}

		public long getClicks() {
			return clicks;
		}

	}
}
