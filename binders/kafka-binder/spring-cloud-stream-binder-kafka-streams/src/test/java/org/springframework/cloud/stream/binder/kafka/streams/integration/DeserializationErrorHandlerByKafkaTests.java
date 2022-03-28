/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
@DirtiesContext
public abstract class DeserializationErrorHandlerByKafkaTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"abc-DeserializationErrorHandlerByKafkaTests-In",
			"xyz-DeserializationErrorHandlerByKafkaTests-In",
			"DeserializationErrorHandlerByKafkaTests-out",
			"error.abc-DeserializationErrorHandlerByKafkaTests-In.group",
			"error.xyz-DeserializationErrorHandlerByKafkaTests-In.group",
			"error.word1.groupx",
			"error.word2.groupx");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	@SpyBean
	org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsMessageConversionDelegate conversionDelegate;

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setUp() {
		System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers",
				embeddedKafka.getBrokersAsString());

		System.setProperty("server.port", "0");
		System.setProperty("spring.jmx.enabled", "false");

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("fooc", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "DeserializationErrorHandlerByKafkaTests-out", "DeserializationErrorHandlerByKafkaTests-out");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
		System.clearProperty("spring.cloud.stream.kafka.streams.binder.brokers");
		System.clearProperty("server.port");
		System.clearProperty("spring.jmx.enabled");
	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.destination=abc-DeserializationErrorHandlerByKafkaTests-In",
			"spring.cloud.stream.bindings.output.destination=DeserializationErrorHandlerByKafkaTests-Out",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id=deser-kafka-dlq",
			"spring.cloud.stream.bindings.input.group=group",
			"spring.cloud.stream.kafka.streams.binder.deserializationExceptionHandler=sendToDlq",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.valueSerde="
					+ "org.apache.kafka.common.serialization.Serdes$IntegerSerde" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
	public static class DeserializationByKafkaAndDlqTests
			extends DeserializationErrorHandlerByKafkaTests {

		@Test
		@Ignore
		public void test() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("abc-DeserializationErrorHandlerByKafkaTests-In");
			template.sendDefault(1, null, "foobar");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobar",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer1, "error.abc-DeserializationErrorHandlerByKafkaTests-In.group");

			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1,
					"error.abc-DeserializationErrorHandlerByKafkaTests-In.group");
			assertThat(cr.value()).isEqualTo("foobar");
			assertThat(cr.partition()).isEqualTo(0); // custom partition function

			// Ensuring that the deserialization was indeed done by Kafka natively
			verify(conversionDelegate, never()).deserializeOnInbound(any(Class.class),
					any(KStream.class));
			verify(conversionDelegate, never()).serializeOnOutbound(any(KStream.class));
		}

	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.destination=xyz-DeserializationErrorHandlerByKafkaTests-In",
			"spring.cloud.stream.bindings.output.destination=DeserializationErrorHandlerByKafkaTests-Out",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id=deser-kafka-dlq",
			"spring.cloud.stream.bindings.input.group=group",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.deserializationExceptionHandler=sendToDlq",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.valueSerde="
					+ "org.apache.kafka.common.serialization.Serdes$IntegerSerde" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
	public static class DeserializationByKafkaAndDlqPerBindingTests
			extends DeserializationErrorHandlerByKafkaTests {

		@Test
		public void test() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("xyz-DeserializationErrorHandlerByKafkaTests-In");
			template.sendDefault(1, null, "foobar");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobar",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer1, "error.xyz-DeserializationErrorHandlerByKafkaTests-In.group");

			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1,
					"error.xyz-DeserializationErrorHandlerByKafkaTests-In.group");
			assertThat(cr.value()).isEqualTo("foobar");
			assertThat(cr.partition()).isEqualTo(0); // custom partition function

			// Ensuring that the deserialization was indeed done by Kafka natively
			verify(conversionDelegate, never()).deserializeOnInbound(any(Class.class),
					any(KStream.class));
			verify(conversionDelegate, never()).serializeOnOutbound(any(KStream.class));
		}

	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.destination=word1,word2",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id=deser-kafka-dlq-multi-input",
			"spring.cloud.stream.bindings.input.group=groupx",
			"spring.cloud.stream.kafka.streams.binder.serdeError=sendToDlq",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.valueSerde="
					+ "org.apache.kafka.common.serialization.Serdes$IntegerSerde" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
	// @checkstyle:on
	public static class DeserializationByKafkaAndDlqTestsWithMultipleInputs
			extends DeserializationErrorHandlerByKafkaTests {

		@Test
		public void test() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("word1");
			template.sendDefault("foobar");

			template.setDefaultTopic("word2");
			template.sendDefault("foobar");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobarx",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromEmbeddedTopics(consumer1, "error.word1.groupx",
					"error.word2.groupx");

			ConsumerRecord<String, String> cr1 = KafkaTestUtils.getSingleRecord(consumer1,
					"error.word1.groupx");
			assertThat(cr1.value()).isEqualTo("foobar");
			ConsumerRecord<String, String> cr2 = KafkaTestUtils.getSingleRecord(consumer1,
					"error.word2.groupx");
			assertThat(cr2.value()).isEqualTo("foobar");

			// Ensuring that the deserialization was indeed done by Kafka natively
			verify(conversionDelegate, never()).deserializeOnInbound(any(Class.class),
					any(KStream.class));
			verify(conversionDelegate, never()).serializeOnOutbound(any(KStream.class));
		}

	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/kstream/integTest-1.properties")
	public static class WordCountProcessorApplication {

		@StreamListener("input")
		@SendTo("output")
		public KStream<?, String> process(KStream<Object, String> input) {

			return input
					.flatMapValues(
							value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofMillis(5000))).count(Materialized.as("foo-WordCounts-x"))
					.toStream().map((key, value) -> new KeyValue<>(null,
							"Count for " + key.key() + " : " + value));
		}

		@Bean
		public DlqPartitionFunction partitionFunction() {
			return (group, rec, ex) -> 0;
		}

	}

}
