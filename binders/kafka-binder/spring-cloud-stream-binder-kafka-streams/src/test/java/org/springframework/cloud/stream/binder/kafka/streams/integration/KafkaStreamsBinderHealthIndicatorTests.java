/*
 * Copyright 2019-2024 the original author or authors.
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsBinderHealthIndicator;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Arnaud Jardiné
 * @author Chris Bono
 */
@EmbeddedKafka(topics = {"out", "out2"})
class KafkaStreamsBinderHealthIndicatorTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		System.setProperty("logging.level.org.apache.kafka", "OFF");
	}

	@Test
	void healthIndicatorUpTest() throws Exception {
		try (ConfigurableApplicationContext context = singleStream("ApplicationHealthTest-xyz")) {
			receive(context,
					Lists.newArrayList(new ProducerRecord<>("in", "{\"id\":\"123\"}"),
							new ProducerRecord<>("in", "{\"id\":\"123\"}")),
					Status.UP, "out");
		}
	}

	@Test
	void healthIndicatorUpMultipleCallsTest() throws Exception {
		try (ConfigurableApplicationContext context = singleStream("ApplicationHealthTest-xyz")) {
			int callsToPerform = 5;
			for (int i = 0; i < callsToPerform; i++) {
				receive(context,
						Lists.newArrayList(new ProducerRecord<>("in", "{\"id\":\"123\"}"),
								new ProducerRecord<>("in", "{\"id\":\"123\"}")),
						Status.UP, "out");
			}
		}
	}

	@Test
	public void healthIndicatorDownTest() throws Exception {
		try (ConfigurableApplicationContext context = singleStream("ApplicationHealthTest-xyzabc")) {
			receive(context,
					Lists.newArrayList(new ProducerRecord<>("in", "{\"id\":\"123\"}"),
							new ProducerRecord<>("in", "{\"id\":\"124\"}")),
					Status.DOWN, "out");
		}
	}

	@Test
	public void healthIndicatorUpMultipleKStreamsTest() throws Exception {
		try (ConfigurableApplicationContext context = multipleStream()) {
			receive(context,
					Lists.newArrayList(new ProducerRecord<>("in", "{\"id\":\"123\"}"),
							new ProducerRecord<>("in2", "{\"id\":\"123\"}")),
					Status.UP, "out", "out2");
		}
	}

	@Test
	public void healthIndicatorDownMultipleKStreamsTest() throws Exception {
		try (ConfigurableApplicationContext context = multipleStream()) {
			receive(context,
					Lists.newArrayList(new ProducerRecord<>("in", "{\"id\":\"123\"}"),
							new ProducerRecord<>("in2", "{\"id\":\"124\"}")),
					Status.DOWN, "out", "out2");
		}
	}

	private static boolean waitFor(Status status, Map<String, Object> details) {
		if (status == Status.UP) {
			String threadState = (String) details.get("threadState");
			return threadState != null
					&& (threadState.equalsIgnoreCase(KafkaStreams.State.REBALANCING.name())
							|| threadState.equalsIgnoreCase("PARTITIONS_REVOKED")
							|| threadState.equalsIgnoreCase("PARTITIONS_ASSIGNED")
							|| threadState.equalsIgnoreCase(
									KafkaStreams.State.PENDING_SHUTDOWN.name()));
		}
		return false;
	}

	private void receive(ConfigurableApplicationContext context,
			List<ProducerRecord<Integer, String>> records, Status expected,
			String... topics) throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-id0",
				"false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		try (Consumer<String, String> consumer = cf.createConsumer()) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			CountDownLatch latch = new CountDownLatch(records.size());
			for (ProducerRecord<Integer, String> record : records) {
				CompletableFuture<SendResult<Integer, String>> future = template.send(record);
				future.whenComplete((result, ex) -> {
					if (ex != null) {
						Assertions.fail();
					}
					else {
						latch.countDown();
					}
				});
			}
			latch.await(5, TimeUnit.SECONDS);

			embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
			KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));

			TimeUnit.SECONDS.sleep(5);
			checkHealth(context, expected);
		}
		finally {
			pf.destroy();
		}
	}

	private static void checkHealth(ConfigurableApplicationContext context,
			Status expected) throws InterruptedException {
		CompositeHealthContributor healthIndicator = context
				.getBean("bindersHealthContributor", CompositeHealthContributor.class);
		KafkaStreamsBinderHealthIndicator kafkaStreamsBinderHealthIndicator = (KafkaStreamsBinderHealthIndicator) healthIndicator.getContributor("kstream");
		Health health = kafkaStreamsBinderHealthIndicator.health();
		while (waitFor(health.getStatus(), health.getDetails())) {
			TimeUnit.SECONDS.sleep(2);
			health = kafkaStreamsBinderHealthIndicator.health();
		}
		assertThat(health.getStatus()).isEqualTo(expected);
	}

	private ConfigurableApplicationContext singleStream(String applicationId) {
		SpringApplication app = new SpringApplication(KStreamApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		return app.run("--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.bindings.process-in-0=input",
				"--spring.cloud.stream.function.bindings.process-out-0=output",
				"--spring.cloud.stream.bindings.input.destination=in",
				"--spring.cloud.stream.bindings.output.destination=out",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde="
						+ "org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde="
						+ "org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId="
						+ applicationId,
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
	}

	private ConfigurableApplicationContext multipleStream() {
		System.setProperty("logging.level.org.apache.kafka", "OFF");
		SpringApplication app = new SpringApplication(AnotherKStreamApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		return app.run("--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=process;process2",
				"--spring.cloud.stream.function.bindings.process-in-0=input",
				"--spring.cloud.stream.function.bindings.process-out-0=output",
				"--spring.cloud.stream.function.bindings.process2-in-0=input2",
				"--spring.cloud.stream.function.bindings.process2-out-0=output2",
				"--spring.cloud.stream.bindings.input.destination=in",
				"--spring.cloud.stream.bindings.output.destination=out",
				"--spring.cloud.stream.bindings.input2.destination=in2",
				"--spring.cloud.stream.bindings.output2.destination=out2",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde="
						+ "org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde="
						+ "org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId="
						+ "ApplicationHealthTest-xyz",
				"--spring.cloud.stream.kafka.streams.bindings.input2.consumer.applicationId="
						+ "ApplicationHealthTest2-xyz",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
	}

	@EnableAutoConfiguration
	public static class KStreamApplication {

		@Bean
		public Function<KStream<Object, Product>, KStream<Object, Product>> process() {
			return input -> input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

	}

	@EnableAutoConfiguration
	public static class AnotherKStreamApplication {

		@Bean
		public Function<KStream<Object, Product>, KStream<Object, Product>> process() {
			return input -> input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

		@Bean
		public Function<KStream<Object, Product>, KStream<Object, Product>> process2() {
			return input -> input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

		@Bean
		public StreamsBuilderFactoryBeanConfigurer customizer() {
			return factoryBean -> {
				factoryBean.setKafkaStreamsCustomizer(kafkaStreams -> kafkaStreams.setUncaughtExceptionHandler(exception ->
						StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT));
			};
		}

	}

	public static class Product {

		Integer id;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

	}
}
