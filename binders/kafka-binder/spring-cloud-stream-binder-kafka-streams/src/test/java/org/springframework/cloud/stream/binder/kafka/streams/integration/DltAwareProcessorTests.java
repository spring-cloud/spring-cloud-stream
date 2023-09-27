/*
 * Copyright 2023-2023 the original author or authors.
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

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.DltAwareProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.DltPublishingContext;
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
@EmbeddedKafka(topics = "hello-dlt-1")
public class DltAwareProcessorTests {

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
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "hello-dlt-1");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void testDltAwareProcessor() {
		SpringApplication app = new SpringApplication(
			DltAwareProcessorTests.PublishToDltOnErrorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
			"--spring.cloud.stream.bindings.errorStream-in-0.destination=error-stream-in",
			"--spring.cloud.stream.kafka.streams.bindings.errorStream-in-0.consumer.application-id=test-error-stream-app-id",
			"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
				+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
				+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"--spring.cloud.stream.kafka.streams.binder.brokers="
				+ embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("error-stream-in", "hello-dlt-1");
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
			assertThat(cr.value().contains("foobar")).isTrue();
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	static class PublishToDltOnErrorApplication {

		@Bean
		public java.util.function.Consumer<KStream<String, String>> errorStream(DltPublishingContext dltSenderContext) {
			return input -> input
				.process(() -> new DltAwareProcessor<>(rec -> {
					throw new RuntimeException("error");
				}, "hello-dlt-1", dltSenderContext));
		}
	}
}
