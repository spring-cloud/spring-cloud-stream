/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsApplicationSupportProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaStreamsBinderWordCountFunctionTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, "counts");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "counts");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	@EnableConfigurationProperties(KafkaStreamsApplicationSupportProperties.class)
	static class WordCountProcessorApplication {

		@Bean
		public Function<KStream<Object, String>, KStream<?, WordCount>> process() {

			return input -> input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(5000))
					.count(Materialized.as("foo-WordCounts"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))));
		}
	}

	@Test
	public void testKstreamWordCountFunction() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.streams.function.definition=process",
				"--spring.cloud.stream.bindings.input.destination=words",
				"--spring.cloud.stream.bindings.output.destination=counts",
				"--spring.cloud.stream.bindings.output.contentType=application/json",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=basic-word-count",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.bindings.output.producer.headerMode=raw",
				"--spring.cloud.stream.bindings.input.consumer.headerMode=raw",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate(context);
		}
	}

	private void receiveAndValidate(ConfigurableApplicationContext context) throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("words");
			template.sendDefault("foobar");
			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, "counts");
			assertThat(cr.value().contains("\"word\":\"foobar\",\"count\":1")).isTrue();
		} finally {
			pf.destroy();
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
