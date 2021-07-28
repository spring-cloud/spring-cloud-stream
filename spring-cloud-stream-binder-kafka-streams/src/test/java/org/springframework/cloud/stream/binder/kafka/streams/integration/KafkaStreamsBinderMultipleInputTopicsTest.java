/*
 * Copyright 2017-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test case demonstrates a kafk-streams topology which consumes messages from
 * multiple kafka topics(destinations).
 *
 * See
 * {@link KafkaStreamsBinderMultipleInputTopicsTest#testKstreamWordCountWithStringInputAndPojoOuput}
 * where the input topic names are specified as comma-separated String values for the
 * property spring.cloud.stream.bindings.input.destination.
 *
 * @author Sarath Shyam
 */
public class KafkaStreamsBinderMultipleInputTopicsTest {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"counts");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "counts");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testKstreamWordCountWithStringInputAndPojoOuput() throws Exception {
		SpringApplication app = new SpringApplication(
				WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=words1,words2",
				"--spring.cloud.stream.bindings.output.destination=counts",
				"--spring.cloud.stream.bindings.output.contentType=application/json",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.timeWindow.length=5000",
				"--spring.cloud.stream.kafka.streams.timeWindow.advanceBy=0",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId"
						+ "=WordCountProcessorApplication-xyz",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
		try {
			receiveAndValidate();
		}
		finally {
			context.close();
		}
	}

	private void receiveAndValidate()
			throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("words1");
		template.sendDefault("foobar1");
		template.setDefaultTopic("words2");
		template.sendDefault("foobar2");
		// Sleep a bit so that both the messages are processed before reading from the
		// output topic.
		// Else assertions might fail arbitrarily.
		Thread.sleep(5000);
		ConsumerRecords<String, String> received = KafkaTestUtils.getRecords(consumer);
		List<String> wordCounts = new ArrayList<>(2);

		received.records("counts")
				.forEach((consumerRecord) -> wordCounts.add((consumerRecord.value())));
		System.out.println(wordCounts);
		assertThat(wordCounts.contains("{\"word\":\"foobar1\",\"count\":1}")).isTrue();
		assertThat(wordCounts.contains("{\"word\":\"foobar2\",\"count\":1}")).isTrue();
	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	static class WordCountProcessorApplication {

		@StreamListener
		@SendTo("output")
		public KStream<?, WordCount> process(
				@Input("input") KStream<Object, String> input) {

			input.map((k, v) -> {
				System.out.println(k);
				System.out.println(v);
				return new KeyValue<>(k, v);
			});
			return input
					.flatMapValues(
							value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
					.count(Materialized.as("WordCounts-tKWCWSIAP0")).toStream()
					.map((key, value) -> new KeyValue<>(null, new WordCount(key, value)));
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(false, true);
		}

	}

	static class WordCount {

		private String word;

		private long count;

		WordCount(String word, long count) {
			this.word = word;
			this.count = count;
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

	}

}
