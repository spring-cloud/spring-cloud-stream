/*
 * Copyright 2017-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gary Russell
 */
@EmbeddedKafka(topics = "counts-1")
class KafkaStreamsBinderTombstoneTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static Consumer<String, String> consumer;

	@BeforeAll
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "counts-1");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void testSendToTombstone()
			throws Exception {
		SpringApplication app = new SpringApplication(
				WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-1",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-1",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.application-id=testKstreamWordCountWithInputBindingLevelApplicationId",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.process-out-0.producer.valueSerde=org.springframework.kafka.support.serializer.JsonSerde",
				"--spring.cloud.stream.bindings.process-in-0.consumer.concurrency=2",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("words-1", "counts-1");
			// Assertions on StreamBuilderFactoryBean
			StreamsBuilderFactoryBean streamsBuilderFactoryBean = context
					.getBean("&stream-builder-process", StreamsBuilderFactoryBean.class);
			KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
			assertThat(kafkaStreams).isNotNull();
			// Ensure that concurrency settings are mapped to number of stream task
			// threads in Kafka Streams.
			final Properties streamsConfiguration = streamsBuilderFactoryBean.getStreamsConfiguration();
			final Integer concurrency = (Integer) streamsConfiguration
					.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG);
			assertThat(concurrency).isEqualTo(2);

			sendTombStoneRecordsAndVerifyGracefulHandling();

			CleanupConfig cleanup = TestUtils.getPropertyValue(streamsBuilderFactoryBean,
					"cleanupConfig", CleanupConfig.class);
			assertThat(cleanup.cleanupOnStart()).isTrue();
			assertThat(cleanup.cleanupOnStop()).isFalse();
		}
	}

	private void receiveAndValidate(String in, String out) {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic(in);
			template.sendDefault("foobar");
			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer,
					out);
			assertThat(cr.value().contains("\"word\":\"foobar\",\"count\":1")).isTrue();
		}
		finally {
			pf.destroy();
		}
	}

	private void sendTombStoneRecordsAndVerifyGracefulHandling() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("words-1");
			template.sendDefault(null);
			ConsumerRecords<String, String> received = consumer
					.poll(Duration.ofMillis(5000));
			// By asserting that the received record is empty, we are ensuring that the
			// tombstone record
			// was handled by the binder gracefully.
			assertThat(received.isEmpty()).isTrue();
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	static class WordCountProcessorApplication {

		@Bean
		public Function<KStream<Object, String>, KStream<String, WordCount>> process() {

			return input -> input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
					.count(Materialized.as("foo-WordCounts"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value,
							new Date(key.window().start()), new Date(key.window().end()))));
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(true, false);
		}

	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}

	}

}
