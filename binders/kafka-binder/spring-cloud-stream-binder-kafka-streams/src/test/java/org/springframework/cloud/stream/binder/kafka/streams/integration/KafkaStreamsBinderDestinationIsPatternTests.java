/*
 * Copyright 2019-present the original author or authors.
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
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.kstream.KStream;
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
 * @author Michael Stoettinger
 */
@EmbeddedKafka(topics = {"in.1", "in.2", "out"})
class KafkaStreamsBinderDestinationIsPatternTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	private static org.apache.kafka.clients.consumer.Consumer<Integer, String> consumer;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "true",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "out");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void destinationIsPattern() {
		SpringApplication app = new SpringApplication(ConsumingApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.cloud.stream.bindings.process-out-0.destination=out",
				"--spring.cloud.stream.bindings.process-in-0.destination=in.*",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.destinationIsPattern=true",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
		try {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
					senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory, true);

			// send message to both topics that fit the pattern
			template.send("in.1", "foo1");
			assertThat(KafkaTestUtils.getSingleRecord(consumer, "out").value())
					.isEqualTo("foo1");
			template.send("in.2", "foo2");
			assertThat(KafkaTestUtils.getSingleRecord(consumer, "out").value())
					.isEqualTo("foo2");
		}
		finally {
			context.close();
		}
	}

	@EnableAutoConfiguration
	public static class ConsumingApplication {

		@Bean
		public Function<KStream<Integer, String>, KStream<Integer, String>> process() {
			return input -> input;
		}

	}

}
