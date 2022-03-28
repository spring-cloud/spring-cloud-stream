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
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
@DirtiesContext
public abstract class DeserializtionErrorHandlerByBinderTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"foos", "goos",
			"counts-id", "error.foos.foobar-group", "error.goos.foobar-group", "error.foos1.fooz-group",
			"error.foos2.fooz-group");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	@SpyBean
	org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsMessageConversionDelegate conversionDelegate;

	private static Consumer<Integer, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers",
				embeddedKafka.getBrokersAsString());
		System.setProperty("server.port", "0");
		System.setProperty("spring.jmx.enabled", "false");

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("kafka-streams-dlq-tests", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "counts-id");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
		System.clearProperty("spring.cloud.stream.kafka.streams.binder.brokers");
		System.clearProperty("server.port");
		System.clearProperty("spring.jmx.enabled");
	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.consumer.useNativeDecoding=false",
			"spring.cloud.stream.bindings.output.producer.useNativeEncoding=false",
			"spring.cloud.stream.bindings.input.destination=foos",
			"spring.cloud.stream.bindings.output.destination=counts-id",
			"spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
					+ "=org.apache.kafka.common.serialization.Serdes$IntegerSerde",
			"spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
					+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"spring.cloud.stream.kafka.streams.binder.deserializationExceptionHandler=sendToDlq",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id"
					+ "=deserializationByBinderAndDlqTests",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.dlqPartitions=1",
			"spring.cloud.stream.bindings.input.group=foobar-group" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
	public static class DeserializationByBinderAndDlqTests
			extends DeserializtionErrorHandlerByBinderTests {

		@Test
		@Ignore
		public void test() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("foos");
			template.sendDefault(1, 7, "hello");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobar",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer1,
					"error.foos.foobar-group");

			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1,
					"error.foos.foobar-group");
			assertThat(cr.value()).isEqualTo("hello");
			assertThat(cr.partition()).isEqualTo(0);

			// Ensuring that the deserialization was indeed done by the binder
			verify(conversionDelegate).deserializeOnInbound(any(Class.class),
					any(KStream.class));
		}
	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.consumer.useNativeDecoding=false",
			"spring.cloud.stream.bindings.output.producer.useNativeEncoding=false",
			"spring.cloud.stream.bindings.input.destination=goos",
			"spring.cloud.stream.bindings.output.destination=counts-id",
			"spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
					+ "=org.apache.kafka.common.serialization.Serdes$IntegerSerde",
			"spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
					+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.deserializationExceptionHandler=sendToDlq",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id"
					+ "=deserializationByBinderAndDlqTests",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.dlqPartitions=1",
			"spring.cloud.stream.bindings.input.group=foobar-group" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
	public static class DeserializationByBinderAndDlqSetOnConsumerBindingTests
			extends DeserializtionErrorHandlerByBinderTests {

		@Test
		public void test() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("goos");
			template.sendDefault(1, 7, "hello");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobar",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer1,
					"error.goos.foobar-group");

			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1,
					"error.goos.foobar-group");
			assertThat(cr.value()).isEqualTo("hello");
			assertThat(cr.partition()).isEqualTo(0);

			// Ensuring that the deserialization was indeed done by the binder
			verify(conversionDelegate).deserializeOnInbound(any(Class.class),
					any(KStream.class));
		}
	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.consumer.useNativeDecoding=false",
			"spring.cloud.stream.bindings.output.producer.useNativeEncoding=false",
			"spring.cloud.stream.bindings.input.destination=foos1,foos2",
			"spring.cloud.stream.bindings.output.destination=counts-id",
			"spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
					+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
					+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"spring.cloud.stream.kafka.streams.binder.serdeError=sendToDlq",
			"spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id"
					+ "=deserializationByBinderAndDlqTestsWithMultipleInputs",
			"spring.cloud.stream.bindings.input.group=fooz-group" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
	public static class DeserializationByBinderAndDlqTestsWithMultipleInputs
			extends DeserializtionErrorHandlerByBinderTests {

		@Test
		@SuppressWarnings("unchecked")
		public void test() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("foos1");
			template.sendDefault("hello");

			template.setDefaultTopic("foos2");
			template.sendDefault("hello");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobar1",
					"false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
					consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromEmbeddedTopics(consumer1, "error.foos1.fooz-group",
					"error.foos2.fooz-group");

			ConsumerRecord<String, String> cr1 = KafkaTestUtils.getSingleRecord(consumer1,
					"error.foos1.fooz-group");
			assertThat(cr1.value().equals("hello")).isTrue();

			ConsumerRecord<String, String> cr2 = KafkaTestUtils.getSingleRecord(consumer1,
					"error.foos2.fooz-group");
			assertThat(cr2.value().equals("hello")).isTrue();

			// Ensuring that the deserialization was indeed done by the binder
			verify(conversionDelegate).deserializeOnInbound(any(Class.class),
					any(KStream.class));
		}

	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	public static class ProductCountApplication {

		@StreamListener("input")
		@SendTo("output")
		public KStream<Integer, Long> process(KStream<Object, Product> input) {
			return input.filter((key, product) -> product.getId() == 123)
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(new JsonSerde<>(Product.class),
							new JsonSerde<>(Product.class)))
					.windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
					.count(Materialized.as("id-count-store-x")).toStream()
					.map((key, value) -> new KeyValue<>(key.key().id, value));
		}

	}

	static class Product {

		Integer id;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

	}

}
