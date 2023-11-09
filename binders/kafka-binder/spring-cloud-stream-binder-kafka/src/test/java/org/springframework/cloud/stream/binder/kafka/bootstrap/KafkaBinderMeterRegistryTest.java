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

package org.springframework.cloud.stream.binder.kafka.bootstrap;

import java.util.Map;
import java.util.function.Function;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
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
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Soby Chacko
 */
@EmbeddedKafka(count = 1, controlledShutdown = true, partitions = 10, topics = "outputTopic")
class KafkaBinderMeterRegistryTest {

	private static EmbeddedKafkaBroker embeddedKafka;

	private static Consumer<String, String> consumer;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "outputTopic");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void metricsWithSingleBinder() throws Exception {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleApplication.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.uppercase-in-0.destination=inputTopic",
						"--spring.cloud.stream.bindings.uppercase-in-0.group=inputGroup",
						"--spring.cloud.stream.bindings.uppercase-out-0.destination=outputTopic",
						"--spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getBrokersAsString());


		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);

		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("inputTopic");
		template.sendDefault("foo");

		// Forcing the retrieval of the data on the outbound so that the producer factory has
		// a chance to add the micrometer listener properly. Only on the first send, binder's
		// internal KafkaTemplate adds the Micrometer listener (using the producer factory).
		KafkaTestUtils.getSingleRecord(consumer, "outputTopic");

		final MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry.class);
		assertMeterRegistry(meterRegistry);
		applicationContext.close();
	}

	@Test
	void metricsWithMultiBinders() {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(SimpleApplication.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.uppercase-in-0.destination=inputTopic",
						"--spring.cloud.stream.bindings.uppercase-in-0.group=inputGroup",
						"--spring.cloud.stream.bindings.uppercase-in-0.binder=kafka1",
						"--spring.cloud.stream.bindings.uppercase-out-0.destination=outputTopic",
						"--spring.cloud.stream.bindings.uppercase-out-0.binder=kafka2",
						"--spring.cloud.stream.binders.kafka1.type=kafka",
						"--spring.cloud.stream.binders.kafka2.type=kafka",
						"--spring.cloud.stream.binders.kafka1.environment"
								+ ".spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getBrokersAsString(),
						"--spring.cloud.stream.binders.kafka2.environment"
								+ ".spring.cloud.stream.kafka.binder.brokers" + "="
								+ embeddedKafka.getBrokersAsString());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);

		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("inputTopic");
		template.sendDefault("foo");

		// Forcing the retrieval of the data on the outbound so that the producer factory has
		// a chance to add the micrometer listener properly. Only on the first send, binder's
		// internal KafkaTemplate adds the Micrometer listener (using the producer factory).
		KafkaTestUtils.getSingleRecord(consumer, "outputTopic");

		final MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry.class);
		assertMeterRegistry(meterRegistry);
		applicationContext.close();
	}

	private void assertMeterRegistry(MeterRegistry meterRegistry) {
		assertThat(meterRegistry).isNotNull();

		// assert kafka binder metrics
		assertThat(meterRegistry.get("spring.cloud.stream.binder.kafka.offset")
				.tag("group", "inputGroup")
				.tag("topic", "inputTopic").gauge().value()).isNotNull();

		// assert consumer metrics
		assertThatCode(() -> meterRegistry.get("kafka.consumer.fetch.manager.fetch.total").meter()).doesNotThrowAnyException();

		// assert producer metrics
		assertThatCode(() -> meterRegistry.get("kafka.producer.io.ratio").meter()).doesNotThrowAnyException();
	}

	@SpringBootApplication
	static class SimpleApplication {

		@Bean
		public Function<String, String> uppercase() {
			return String::toUpperCase;
		}
	}
}
