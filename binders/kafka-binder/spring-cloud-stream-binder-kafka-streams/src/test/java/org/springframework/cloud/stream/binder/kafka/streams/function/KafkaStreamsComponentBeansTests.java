/*
 * Copyright 2021-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@EmbeddedKafka(topics = {"testFunctionComponent-out", "testBiFunctionComponent-out", "testCurriedFunctionWithFunctionTerminal-out"})
class KafkaStreamsComponentBeansTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static Consumer<String, String> consumer1;
	private static Consumer<String, String> consumer2;
	private static Consumer<String, String> consumer3;

	private final static CountDownLatch LATCH_1 = new CountDownLatch(1);
	private final static CountDownLatch LATCH_2 = new CountDownLatch(2);
	private final static CountDownLatch LATCH_3 = new CountDownLatch(3);

	@BeforeAll
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer1 = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer1, "testFunctionComponent-out");

		Map<String, Object> consumerProps1 = KafkaTestUtils.consumerProps("group-x", "false",
				embeddedKafka);
		consumerProps1.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		DefaultKafkaConsumerFactory<String, String> cf1 = new DefaultKafkaConsumerFactory<>(consumerProps1);
		consumer2 = cf1.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer2, "testBiFunctionComponent-out");

		Map<String, Object> consumerProps2 = KafkaTestUtils.consumerProps("group-y", "false",
				embeddedKafka);
		consumerProps2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		DefaultKafkaConsumerFactory<String, String> cf2 = new DefaultKafkaConsumerFactory<>(consumerProps2);
		consumer3 = cf2.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer3, "testCurriedFunctionWithFunctionTerminal-out");
	}

	@AfterAll
	public static void tearDown() {
		consumer1.close();
		consumer2.close();
		consumer3.close();
	}

	@Test
	void testFunctionComponent() {
		SpringApplication app = new SpringApplication(FunctionAsComponent.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext ignored = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.foo-in-0.destination=testFunctionComponent-in",
				"--spring.cloud.stream.bindings.foo-out-0.destination=testFunctionComponent-out",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("testFunctionComponent-in");
				template.sendDefault("foobar");
				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1, "testFunctionComponent-out");
				assertThat(cr.value().contains("foobarfoobar")).isTrue();
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testConsumerComponent() throws Exception {
		SpringApplication app = new SpringApplication(ConsumerAsComponent.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.bar-in-0.destination=testConsumerComponent-in",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("testConsumerComponent-in");
				template.sendDefault("foobar");
				Assert.isTrue(LATCH_1.await(10, TimeUnit.SECONDS), "bar");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testBiFunctionComponent() {
		SpringApplication app = new SpringApplication(BiFunctionAsComponent.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext ignored = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.bazz-in-0.destination=testBiFunctionComponent-in-0",
				"--spring.cloud.stream.bindings.bazz-in-1.destination=testBiFunctionComponent-in-1",
				"--spring.cloud.stream.bindings.bazz-out-0.destination=testBiFunctionComponent-out",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("testBiFunctionComponent-in-0");
				template.sendDefault("foobar");
				template.setDefaultTopic("testBiFunctionComponent-in-1");
				template.sendDefault("foobar");
				final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer2, Duration.ofSeconds(10), 2);
				assertThat(records.count()).isEqualTo(2);
				records.forEach(stringStringConsumerRecord -> assertThat(stringStringConsumerRecord.value().contains("foobar")).isTrue());
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testBiConsumerComponent() throws Exception {
		SpringApplication app = new SpringApplication(BiConsumerAsComponent.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.buzz-in-0.destination=testBiConsumerComponent-in-0",
				"--spring.cloud.stream.bindings.buzz-in-1.destination=testBiConsumerComponent-in-1",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("testBiConsumerComponent-in-0");
				template.sendDefault("foobar");
				template.setDefaultTopic("testBiConsumerComponent-in-1");
				template.sendDefault("foobar");
				Assert.isTrue(LATCH_2.await(10, TimeUnit.SECONDS), "bar");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testCurriedFunctionWithConsumerTerminal() throws Exception {
		SpringApplication app = new SpringApplication(CurriedFunctionWithConsumerTerminal.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.curriedConsumer-in-0.destination=testCurriedFunctionWithConsumerTerminal-in-0",
				"--spring.cloud.stream.bindings.curriedConsumer-in-1.destination=testCurriedFunctionWithConsumerTerminal-in-1",
				"--spring.cloud.stream.bindings.curriedConsumer-in-2.destination=testCurriedFunctionWithConsumerTerminal-in-2",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("testCurriedFunctionWithConsumerTerminal-in-0");
				template.sendDefault("foobar");
				template.setDefaultTopic("testCurriedFunctionWithConsumerTerminal-in-1");
				template.sendDefault("foobar");
				template.setDefaultTopic("testCurriedFunctionWithConsumerTerminal-in-2");
				template.sendDefault("foobar");
				Assert.isTrue(LATCH_3.await(10, TimeUnit.SECONDS), "bar");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testCurriedFunctionWithFunctionTerminal() {
		SpringApplication app = new SpringApplication(CurriedFunctionWithFunctionTerminal.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.curriedFunction-in-0.destination=testCurriedFunctionWithFunctionTerminal-in-0",
				"--spring.cloud.stream.bindings.curriedFunction-in-1.destination=testCurriedFunctionWithFunctionTerminal-in-1",
				"--spring.cloud.stream.bindings.curriedFunction-in-2.destination=testCurriedFunctionWithFunctionTerminal-in-2",
				"--spring.cloud.stream.bindings.curriedFunction-out-0.destination=testCurriedFunctionWithFunctionTerminal-out",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("testCurriedFunctionWithFunctionTerminal-in-0");
				template.sendDefault("foobar");
				template.setDefaultTopic("testCurriedFunctionWithFunctionTerminal-in-1");
				template.sendDefault("foobar");
				template.setDefaultTopic("testCurriedFunctionWithFunctionTerminal-in-2");
				template.sendDefault("foobar");
				final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer3, Duration.ofSeconds(10), 3);
				assertThat(records.count()).isEqualTo(3);
				records.forEach(stringStringConsumerRecord -> assertThat(stringStringConsumerRecord.value().contains("foobar")).isTrue());
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Component("foo")
	@EnableAutoConfiguration
	public static class FunctionAsComponent implements Function<KStream<Integer, String>,
			KStream<String, String>> {

		@Override
		public KStream<String, String> apply(KStream<Integer, String> stringIntegerKStream) {
			return stringIntegerKStream.map((integer, s) -> new KeyValue<>(s, s + s));
		}
	}

	@Component("bar")
	@EnableAutoConfiguration
	public static class ConsumerAsComponent implements java.util.function.Consumer<KStream<Integer, String>> {

		@Override
		public void accept(KStream<Integer, String> integerStringKStream) {
			integerStringKStream.foreach((integer, s) -> LATCH_1.countDown());
		}
	}

	@Component("bazz")
	@EnableAutoConfiguration
	public static class BiFunctionAsComponent implements BiFunction<KStream<String, String>, KStream<String, String>, KStream<String, String>> {

		@Override
		public KStream<String, String> apply(KStream<String, String> stringStringKStream, KStream<String, String> stringStringKStream2) {
			return stringStringKStream.merge(stringStringKStream2);
		}
	}

	@Component("buzz")
	@EnableAutoConfiguration
	public static class BiConsumerAsComponent implements BiConsumer<KStream<String, String>, KStream<String, String>> {

		@Override
		public void accept(KStream<String, String> stringStringKStream, KStream<String, String> stringStringKStream2) {
			final KStream<String, String> merged = stringStringKStream.merge(stringStringKStream2);
			merged.foreach((s, s2) -> LATCH_2.countDown());
		}
	}

	@Component("curriedConsumer")
	@EnableAutoConfiguration
	public static class CurriedFunctionWithConsumerTerminal implements Function<KStream<String, String>,
												Function<KStream<String, String>,
														java.util.function.Consumer<KStream<String, String>>>> {

		@Override
		public Function<KStream<String, String>, java.util.function.Consumer<KStream<String, String>>> apply(KStream<String, String> stringStringKStream) {
			return stringStringKStream1 -> stringStringKStream2 -> {
				final KStream<String, String> merge1 = stringStringKStream.merge(stringStringKStream1);
				final KStream<String, String> merged2 = merge1.merge(stringStringKStream2);
				merged2.foreach((s1, s2) -> LATCH_3.countDown());
			};
		}
	}

	@Component("curriedFunction")
	@EnableAutoConfiguration
	public static class CurriedFunctionWithFunctionTerminal implements Function<KStream<String, String>,
			Function<KStream<String, String>,
					java.util.function.Function<KStream<String, String>, KStream<String, String>>>> {

		@Override
		public Function<KStream<String, String>, Function<KStream<String, String>, KStream<String, String>>> apply(KStream<String, String> stringStringKStream) {
			return stringStringKStream1 -> stringStringKStream2 -> {
				final KStream<String, String> merge1 = stringStringKStream.merge(stringStringKStream1);
				return merge1.merge(stringStringKStream2);
			};
		}
	}
}
