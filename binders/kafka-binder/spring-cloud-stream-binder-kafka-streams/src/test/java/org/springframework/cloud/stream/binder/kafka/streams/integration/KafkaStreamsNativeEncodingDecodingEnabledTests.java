/*
 * Copyright 2018-present the original author or authors.
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
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@EmbeddedKafka(topics = {"decode-counts", "decode-counts-1"}, partitions = 1)
class KafkaStreamsNativeEncodingDecodingEnabledTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	private static Consumer<String, String> consumer;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "group", false);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "decode-counts-1");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void nativeEncodingDecodingEnabled(EmbeddedKafkaBroker embeddedKafka) {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=decode-words-1",
				"--spring.cloud.stream.bindings.process-out-0.destination=decode-counts-1",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.applicationId=NativeEncodingDecodingEnabledTests-abc",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("decode-words-1");
				template.sendDefault("foobar");
				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer,
						"decode-counts-1");
				assertThat(cr.value().equals("Count for foobar : 1")).isTrue();
			}
			finally {
				pf.destroy();
			}
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
					.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
					.count(Materialized.as("foo-WordCounts-x"))
					.toStream().map((key, value) -> new KeyValue<>(null,
							"Count for " + key.key() + " : " + value));
		}
	}
}
