/*
 * Copyright 2019-2023 the original author or authors.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

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
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = "output-topic-1")
class StreamToTableJoinFunctionTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	@Test
	void testStreamToTable() {
		SpringApplication app = new SpringApplication(CountClicksPerRegionApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		Consumer<String, Long> consumer;
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-1",
				"false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		DefaultKafkaConsumerFactory<String, Long> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "output-topic-1");

		runTest(app, consumer);
	}

	@Test
	void testStreamToTableBiFunction() {
		SpringApplication app = new SpringApplication(BiFunctionCountClicksPerRegionApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		Consumer<String, Long> consumer;
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-2",
				"false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		DefaultKafkaConsumerFactory<String, Long> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "output-topic-1");

		runTest(app, consumer);
	}

	@Test
	void testStreamToTableBiConsumer() throws Exception {
		SpringApplication app = new SpringApplication(BiConsumerApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		Consumer<String, Long> consumer;
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-2",
				"false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		DefaultKafkaConsumerFactory<String, Long> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "output-topic-1");

		try (ConfigurableApplicationContext ignored = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=user-clicks-1",
				"--spring.cloud.stream.bindings.process-in-1.destination=user-regions-1",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=10000",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.applicationId" +
						"=testStreamToTableBiConsumer",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			// Input 1: Region per user (multiple records allowed per user).
			List<KeyValue<String, String>> userRegions = Arrays.asList(
					new KeyValue<>("alice", "asia")
			);

			Map<String, Object> senderProps1 = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			senderProps1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

			DefaultKafkaProducerFactory<String, String> pf1 = new DefaultKafkaProducerFactory<>(senderProps1);
			KafkaTemplate<String, String> template1 = new KafkaTemplate<>(pf1, true);
			template1.setDefaultTopic("user-regions-1");

			for (KeyValue<String, String> keyValue : userRegions) {
				template1.sendDefault(keyValue.key, keyValue.value);
			}

			// Input 2: Clicks per user (multiple records allowed per user).
			List<KeyValue<String, Long>> userClicks = Arrays.asList(
					new KeyValue<>("alice", 13L)
			);

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

			DefaultKafkaProducerFactory<String, Long> pf = new DefaultKafkaProducerFactory<>(senderProps);
			KafkaTemplate<String, Long> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("user-clicks-1");

			for (KeyValue<String, Long> keyValue : userClicks) {
				template.sendDefault(keyValue.key, keyValue.value);
			}

			Assert.isTrue(BiConsumerApplication.latch.await(10, TimeUnit.SECONDS), "Failed to receive message");

		}
		finally {
			consumer.close();
		}
	}

	private void runTest(SpringApplication app, Consumer<String, Long> consumer) {
		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=user-clicks-1",
				"--spring.cloud.stream.bindings.process-in-1.destination=user-regions-1",
				"--spring.cloud.stream.bindings.process-out-0.destination=output-topic-1",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=10000",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.applicationId" +
						"=StreamToTableJoinFunctionTests-abc",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-1.consumer.topic.properties.cleanup.policy=compact",
				"--spring.cloud.stream.kafka.streams.bindings.process-out-0.producer.topic.properties.cleanup.policy=compact",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			// Input 1: Region per user (multiple records allowed per user).
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
			template1.setDefaultTopic("user-regions-1");

			for (KeyValue<String, String> keyValue : userRegions) {
				template1.sendDefault(keyValue.key, keyValue.value);
			}

			// Input 2: Clicks per user (multiple records allowed per user).
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
			template.setDefaultTopic("user-clicks-1");

			for (KeyValue<String, Long> keyValue : userClicks) {
				template.sendDefault(keyValue.key, keyValue.value);
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

			// Testing certain ancillary configuration of GlobalKTable around topics creation.
			// See this issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/687
			BinderFactory binderFactory = context.getBeanFactory()
					.getBean(BinderFactory.class);

			Binder<KTable, ? extends ConsumerProperties, ? extends ProducerProperties> ktableBinder = binderFactory
					.getBinder("ktable", KTable.class);

			KafkaStreamsConsumerProperties inputX = (KafkaStreamsConsumerProperties) ((ExtendedPropertiesBinder) ktableBinder)
					.getExtendedConsumerProperties("process-in-1");
			String cleanupPolicyX = inputX.getTopic().getProperties().get("cleanup.policy");

			assertThat(cleanupPolicyX).isEqualTo("compact");

			Binder<KStream, ? extends ConsumerProperties, ? extends ProducerProperties> kStreamBinder = binderFactory
					.getBinder("kstream", KStream.class);

			KafkaStreamsProducerProperties producerProperties = (KafkaStreamsProducerProperties) ((ExtendedPropertiesBinder) kStreamBinder)
					.getExtendedProducerProperties("process-out-0");

			String cleanupPolicyOutput = producerProperties.getTopic().getProperties().get("cleanup.policy");

			assertThat(cleanupPolicyOutput).isEqualTo("compact");
		}
		finally {
			consumer.close();
		}
	}

//	@Test
	public void testGlobalStartOffsetWithLatestAndIndividualBindingWthEarliest() throws Exception {
		SpringApplication app = new SpringApplication(BiFunctionCountClicksPerRegionApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		Consumer<String, Long> consumer;
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-3",
				"false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		DefaultKafkaConsumerFactory<String, Long> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "output-topic-2");

		// Produce data first to the input topic to test the startOffset setting on the
		// binding (which is set to earliest below).
		// Input 1: Clicks per user (multiple records allowed per user).
		List<KeyValue<String, Long>> userClicks = Arrays.asList(
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L),
				new KeyValue<>("alice", 100L)
		);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

		DefaultKafkaProducerFactory<String, Long> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<String, Long> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("user-clicks-2");

		for (KeyValue<String, Long> keyValue : userClicks) {
			template.sendDefault(keyValue.key, keyValue.value);
		}

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=user-clicks-2",
				"--spring.cloud.stream.bindings.process-in-1.destination=user-regions-2",
				"--spring.cloud.stream.bindings.process-out-0.destination=output-topic-2",
				"--spring.cloud.stream.bindings.process-in-0.consumer.useNativeDecoding=true",
				"--spring.cloud.stream.bindings.process-in-1.consumer.useNativeDecoding=true",
				"--spring.cloud.stream.bindings.process-out-0.producer.useNativeEncoding=true",
				"--spring.cloud.stream.kafka.streams.binder.configuration.auto.offset.reset=latest",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.startOffset=earliest",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=10000",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.application-id" +
						"=StreamToTableJoinFunctionTests-foobar",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Thread.sleep(1000L);

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
			template1.setDefaultTopic("user-regions-2");

			for (KeyValue<String, String> keyValue : userRegions) {
				template1.sendDefault(keyValue.key, keyValue.value);
			}


			// Input 1: Clicks per user (multiple records allowed per user).
			List<KeyValue<String, Long>> userClicks1 = Arrays.asList(
					new KeyValue<>("bob", 4L),
					new KeyValue<>("chao", 25L),
					new KeyValue<>("bob", 19L),
					new KeyValue<>("dave", 56L),
					new KeyValue<>("eve", 78L),
					new KeyValue<>("fang", 99L)
			);

			for (KeyValue<String, Long> keyValue : userClicks1) {
				template.sendDefault(keyValue.key, keyValue.value);
			}

			List<KeyValue<String, Long>> expectedClicksPerRegion = Arrays.asList(
					new KeyValue<>("americas", 101L),
					new KeyValue<>("europe", 56L),
					new KeyValue<>("asia", 124L),
					//1000 alice entries which were there in the topic before the consumer started.
					//Since we set the startOffset to earliest for the topic, it will read them,
					//but the join fails to associate with a valid region, thus UNKNOWN.
					new KeyValue<>("UNKNOWN", 1000L)
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

			// TODO: Matched count is 3 and not 4 (expectedClicksPerRegion.size()) when running with full suite. Investigate why.
			// TODO: This behavior is only observed after the Spring Kafka upgrade to 2.5.0 and kafka client to 2.5.
			// TODO: Note that the test passes fine as a single test.
			assertThat(count).matches(
					matchedCount -> matchedCount == expectedClicksPerRegion.size() - 1 || matchedCount == expectedClicksPerRegion.size());
			assertThat(actualClicksPerRegion).containsAnyElementsOf(expectedClicksPerRegion);
		}
		finally {
			consumer.close();
		}
	}

	@Test
	public void testTrivialSingleKTableInputAsNonDeclarative() {
		SpringApplication app = new SpringApplication(TrivialKTableApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.run("--server.port=0",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.application-id=" +
						"testTrivialSingleKTableInputAsNonDeclarative");
		//All we are verifying is that this application didn't throw any errors.
		//See this issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/536
	}

	@Test
	public void testTwoKStreamsCanBeJoined() {
		SpringApplication app = new SpringApplication(
				JoinProcessor.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.run("--server.port=0",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString(),
				"--spring.application.name=" +
						"two-kstream-input-join-integ-test");
		//All we are verifying is that this application didn't throw any errors.
		//See this issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/701
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

	@EnableAutoConfiguration
	public static class CountClicksPerRegionApplication {

		@Bean
		public Function<KStream<String, Long>, Function<KTable<String, String>, KStream<String, Long>>> process() {
			return userClicksStream -> (userRegionsTable -> (userClicksStream
					.leftJoin(userRegionsTable, (clicks, region) -> new RegionWithClicks(region == null ?
									"UNKNOWN" : region, clicks),
							Joined.with(Serdes.String(), Serdes.Long(), null))
					.map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(),
							regionWithClicks.getClicks()))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
					.reduce(Long::sum, Materialized.as("CountClicks-" + UUID.randomUUID()))
					.toStream()));
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(false, true);
		}
	}

	@EnableAutoConfiguration
	public static class BiFunctionCountClicksPerRegionApplication {

		@Bean
		public BiFunction<KStream<String, Long>, KTable<String, String>, KStream<String, Long>> process() {
			return (userClicksStream, userRegionsTable) -> (userClicksStream
					.leftJoin(userRegionsTable, (clicks, region) -> new RegionWithClicks(region == null ?
									"UNKNOWN" : region, clicks),
							Joined.with(Serdes.String(), Serdes.Long(), null))
					.map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(),
							regionWithClicks.getClicks()))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
					.reduce(Long::sum, Materialized.as("CountClicks-" + UUID.randomUUID()))
					.toStream());
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(false, true);
		}
	}

	@EnableAutoConfiguration
	public static class BiConsumerApplication {

		static CountDownLatch latch = new CountDownLatch(2);

		@Bean
		public BiConsumer<KStream<String, Long>, KTable<String, String>> process() {
			return (userClicksStream, userRegionsTable) -> {
				userClicksStream.foreach((key, value) -> latch.countDown());
				userRegionsTable.toStream().foreach((key, value) -> latch.countDown());
			};
		}
	}

	@EnableAutoConfiguration
	public static class TrivialKTableApp {

		public java.util.function.Consumer<KTable<String, String>> process() {
			return inputTable -> inputTable.toStream().foreach((key, value) -> System.out.println("key : value " + key + " : " + value));
		}
	}

	@EnableAutoConfiguration
	public static class JoinProcessor {

		public BiConsumer<KStream<String, String>, KStream<String, String>> testProcessor() {
			return (input1Stream, input2Stream) -> input1Stream
					.join(input2Stream,
							(event1, event2) -> null,
							JoinWindows.of(Duration.ofMillis(5)),
							StreamJoined.with(
									Serdes.String(),
									Serdes.String(),
									Serdes.String()
							)
					);
		}
	}

}
