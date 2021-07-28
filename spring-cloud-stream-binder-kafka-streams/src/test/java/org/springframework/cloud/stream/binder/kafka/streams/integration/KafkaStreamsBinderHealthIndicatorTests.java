/*
 * Copyright 2019-2019 the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Arnaud Jardiné
 */
public class KafkaStreamsBinderHealthIndicatorTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"out", "out2");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	@BeforeClass
	public static void setUp() {
		System.setProperty("logging.level.org.apache.kafka", "OFF");
	}

	@Test
	public void healthIndicatorUpTest() throws Exception {
		try (ConfigurableApplicationContext context = singleStream("ApplicationHealthTest-xyz")) {
			receive(context,
					Lists.newArrayList(new ProducerRecord<>("in", "{\"id\":\"123\"}"),
							new ProducerRecord<>("in", "{\"id\":\"123\"}")),
					Status.UP, "out");
		}
	}

	@Test
	public void healthIndicatorUpMultipleCallsTest() throws Exception {
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
				ListenableFuture<SendResult<Integer, String>> future = template
						.send(record);
				future.addCallback(
						new ListenableFutureCallback<SendResult<Integer, String>>() {
							@Override
							public void onFailure(Throwable ex) {
								Assert.fail();
							}

							@Override
							public void onSuccess(SendResult<Integer, String> result) {
								latch.countDown();
							}
						});
			}

			latch.await(5, TimeUnit.SECONDS);

			embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
			KafkaTestUtils.getRecords(consumer, 1000);

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

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	public static class KStreamApplication {

		@StreamListener("input")
		@SendTo("output")
		public KStream<Object, Product> process(KStream<Object, Product> input) {
			return input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

		@Bean
		public StreamsBuilderFactoryBeanConfigurer customizer() {
			return factoryBean -> {
				factoryBean.setKafkaStreamsCustomizer(new KafkaStreamsCustomizer() {
					@Override
					public void customize(KafkaStreams kafkaStreams) {
						kafkaStreams.setUncaughtExceptionHandler(exception ->
								StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
					}
				});
			};
		}

	}

	@EnableBinding({ KafkaStreamsProcessor.class, KafkaStreamsProcessorX.class })
	@EnableAutoConfiguration
	public static class AnotherKStreamApplication {

		@StreamListener("input")
		@SendTo("output")
		public KStream<Object, Product> process(KStream<Object, Product> input) {
			return input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

		@StreamListener("input2")
		@SendTo("output2")
		public KStream<Object, Product> process2(KStream<Object, Product> input) {
			return input.filter((key, product) -> {
				if (product.getId() != 123) {
					throw new IllegalArgumentException();
				}
				return true;
			});
		}

		@Bean
		public StreamsBuilderFactoryBeanConfigurer customizer() {
			return factoryBean -> {
				factoryBean.setKafkaStreamsCustomizer(new KafkaStreamsCustomizer() {
					@Override
					public void customize(KafkaStreams kafkaStreams) {
						kafkaStreams.setUncaughtExceptionHandler(exception ->
								StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
					}
				});
			};
		}

	}

	public interface KafkaStreamsProcessorX {

		@Input("input2")
		KStream<?, ?> input();

		@Output("output2")
		KStream<?, ?> output();

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
