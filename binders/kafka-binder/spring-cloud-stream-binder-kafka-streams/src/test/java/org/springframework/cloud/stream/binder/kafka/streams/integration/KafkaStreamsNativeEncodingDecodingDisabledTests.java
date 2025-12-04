/*
 * Copyright 2023-present the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
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
public abstract class KafkaStreamsNativeEncodingDecodingDisabledTests {

	/**
	 * Kafka rule.
	 */
	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
		"decode-counts", "decode-counts-1");

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

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
			embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
			consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "decode-counts", "decode-counts-1");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
		System.clearProperty("spring.cloud.stream.kafka.streams.binder.brokers");
		System.clearProperty("server.port");
		System.clearProperty("spring.jmx.enabled");
	}

	@SpringBootTest(classes = WordCountProcessorApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE,
		properties = {
			"spring.cloud.stream.bindings.process-in-0.destination=decode-words",
			"spring.cloud.stream.bindings.process-out-0.destination=decode-counts",
			"spring.cloud.stream.bindings.process-in-0.consumer.useNativeDecoding=false",
			"spring.cloud.stream.bindings.process-out-0.producer.useNativeEncoding=false",
			"spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.applicationId"
				+ "=hello-NativeEncodingDecodingEnabledTests-xyz" })
	public static class NativeEncodingDecodingDisabledTests
		extends KafkaStreamsNativeEncodingDecodingDisabledTests {

		@Test
		public void nativeEncodingDecodingDisabled() {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("decode-words");
			Message<String> msg = MessageBuilder.withPayload("foobar").setHeader("foo", "bar").build();
			template.send(msg);

			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer,
				"decode-counts");

			final Headers headers = cr.headers();
			final Iterable<Header> foo = headers.headers("foo");
			assertThat(foo.iterator().hasNext()).isTrue();
			final Header fooHeader = foo.iterator().next();
			assertThat(fooHeader.value()).isEqualTo("bar".getBytes(StandardCharsets.UTF_8));

			assertThat(cr.value().equals("Count for foobar : 1")).isTrue();

			verify(conversionDelegate).serializeOnOutbound(any(KStream.class));
			verify(conversionDelegate).deserializeOnInbound(any(Class.class),
				any(KStream.class));
		}

	}

	@EnableAutoConfiguration
	public static class WordCountProcessorApplication {

		@Bean
		public Function<KStream<Object, String>, KStream<?, String>> process() {

			return input -> input
				.flatMapValues(
					value -> Arrays.asList(value.toLowerCase(Locale.ROOT).split("\\W+")))
				.map((key, value) -> new KeyValue<>(value, value))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
				.windowedBy(TimeWindows.of(Duration.ofSeconds(5))).count(Materialized.as("foo-WordCounts-x"))
				.toStream().map((key, value) -> new KeyValue<>(null,
					"Count for " + key.key() + " : " + value));
		}

	}

}
