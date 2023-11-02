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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"fooFuncanotherFooFunc-out-0", "bar"})
class KafkaStreamsFunctionCompositionTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static Consumer<String, String> consumer;

	private static final CountDownLatch countDownLatch1 = new CountDownLatch(1);
	private static final CountDownLatch countDownLatch2 = new CountDownLatch(1);
	private static final CountDownLatch countDownLatch3 = new CountDownLatch(2);

	@BeforeAll
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("fn-composition-group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "fooFuncanotherFooFunc-out-0", "bar");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void testBasicFunctionCompositionWithDefaultDestination() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig1.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=fooFunc|anotherFooFunc;anotherProcess",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("fooFuncanotherFooFunc-in-0");
				template.sendDefault("foobar!!");
				//Verify non-composed funcions can be run standalone with composed function chains, i.e foo|bar;buzz
				template.setDefaultTopic("anotherProcess-in-0");
				template.sendDefault("this is crazy!!!");
				Thread.sleep(1000);

				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, "fooFuncanotherFooFunc-out-0");
				assertThat(cr.value().contains("foobar!!")).isTrue();

				Assert.isTrue(countDownLatch1.await(5, TimeUnit.SECONDS), "anotherProcess consumer didn't trigger.");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testBasicFunctionCompositionWithDestinaion() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig1.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=fooFunc|anotherFooFunc;anotherProcess",
				"--spring.cloud.stream.bindings.fooFuncanotherFooFunc-in-0.destination=foo",
				"--spring.cloud.stream.bindings.fooFuncanotherFooFunc-out-0.destination=bar",
				"--spring.cloud.stream.bindings.anotherProcess-in-0.destination=buzz",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("foo");
				template.sendDefault("foobar!!");
				template.setDefaultTopic("buzz");
				template.sendDefault("this is crazy!!!");
				Thread.sleep(1000);

				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, "bar");
				assertThat(cr.value().contains("foobar!!")).isTrue();

				Assert.isTrue(countDownLatch1.await(5, TimeUnit.SECONDS), "anotherProcess consumer didn't trigger.");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testFunctionToConsumerComposition() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig2.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=fooFunc|anotherProcess",
				"--spring.cloud.stream.bindings.fooFuncanotherProcess-in-0.destination=foo",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("foo");
				template.sendDefault("foobar!!");

				Thread.sleep(1000);

				Assert.isTrue(countDownLatch2.await(5, TimeUnit.SECONDS), "anotherProcess consumer didn't trigger.");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testBiFunctionToConsumerComposition() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig3.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=fooBiFunc|anotherProcess",
				"--spring.cloud.stream.bindings.fooBiFuncanotherProcess-in-0.destination=foo",
				"--spring.cloud.stream.bindings.fooBiFuncanotherProcess-in-1.destination=foo-1",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("foo");
				template.sendDefault("foobar!!");

				template.setDefaultTopic("foo-1");
				template.sendDefault("another foobar!!");

				Thread.sleep(1000);

				Assert.isTrue(countDownLatch3.await(5, TimeUnit.SECONDS), "anotherProcess consumer didn't trigger.");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testChainedFunctionsAsComposed() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig4.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.streams.binder.applicationId=my-app-id",
				"--spring.cloud.function.definition=fooBiFunc|anotherFooFunc|yetAnotherFooFunc|lastFunctionInChain",
				"--spring.cloud.stream.function.bindings.fooBiFuncanotherFooFuncyetAnotherFooFunclastFunctionInChain-in-0=input1",
				"--spring.cloud.stream.function.bindings.fooBiFuncanotherFooFuncyetAnotherFooFunclastFunctionInChain-in-1=input2",
				"--spring.cloud.stream.function.bindings.fooBiFuncanotherFooFuncyetAnotherFooFunclastFunctionInChain-out-0=output",
				"--spring.cloud.stream.bindings.input1.destination=my-foo-1",
				"--spring.cloud.stream.bindings.input2.destination=my-foo-2",
				"--spring.cloud.stream.bindings.output.destination=bar",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);

				template.setDefaultTopic("my-foo-2");
				template.sendDefault("foo-1", "foo2");

				template.setDefaultTopic("my-foo-1");
				template.sendDefault("foo-1", "foo1");

				Thread.sleep(1000);

				final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
				assertThat(records.iterator().hasNext()).isTrue();
				assertThat(records.iterator().next().value().equals("foo1foo2From-anotherFooFuncFrom-yetAnotherFooFuncFrom-lastFunctionInChain")).isTrue();
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testFirstFunctionCurriedThenComposeWithOtherFunctions() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig5.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.streams.binder.applicationId=my-app-id-xyz",
				"--spring.cloud.function.definition=curriedFunc|anotherFooFunc|yetAnotherFooFunc|lastFunctionInChain",
				"--spring.cloud.stream.function.bindings.curriedFuncanotherFooFuncyetAnotherFooFunclastFunctionInChain-in-0=input1",
				"--spring.cloud.stream.function.bindings.curriedFuncanotherFooFuncyetAnotherFooFunclastFunctionInChain-in-1=input2",
				"--spring.cloud.stream.function.bindings.curriedFuncanotherFooFuncyetAnotherFooFunclastFunctionInChain-out-0=output",
				"--spring.cloud.stream.bindings.input1.destination=my-foo-1",
				"--spring.cloud.stream.bindings.input2.destination=my-foo-2",
				"--spring.cloud.stream.bindings.output.destination=bar",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);

				template.setDefaultTopic("my-foo-2");
				template.sendDefault("foo-1", "foo2");

				Thread.sleep(1000);

				template.setDefaultTopic("my-foo-1");
				template.sendDefault("foo-1", "foo1");

				Thread.sleep(1000);

				final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
				assertThat(records.iterator().hasNext()).isTrue();
				assertThat(records.iterator().next().value().equals("foo1foo2From-anotherFooFuncFrom-yetAnotherFooFuncFrom-lastFunctionInChain")).isTrue();
			}
			finally {
				pf.destroy();
			}
		}
	}

	@Test
	void testFunctionToConsumerCompositionWithFunctionProducesKTable() throws InterruptedException {
		SpringApplication app = new SpringApplication(FunctionCompositionConfig6.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=fooFunc|anotherProcess",
				"--spring.cloud.stream.bindings.fooFuncanotherProcess-in-0.destination=foo",
				"--spring.cloud.stream.bindings.fooFuncanotherProcess-out-0.destination=bar",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("foo");
				template.sendDefault("foo", "foobar!!");

				Thread.sleep(1000);

				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, "bar");
				assertThat(cr.value().contains("foobar!!")).isTrue();
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableAutoConfiguration
	public static class FunctionCompositionConfig1 {

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> fooFunc() {
			return input -> input.peek((s, s2) -> {
				System.out.println("hello: " + s2);
			});
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> anotherFooFunc() {
			return input -> input.peek((s, s2) -> System.out.println("hello Foo: " + s2));
		}

		@Bean
		public java.util.function.Consumer<KStream<String, String>> anotherProcess() {
			return c -> c.foreach((s, s2) -> {
				System.out.println("s2s2s2::" + s2);
				countDownLatch1.countDown();
			});
		}
	}

	@EnableAutoConfiguration
	public static class FunctionCompositionConfig2 {

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> fooFunc() {
			return input -> input.peek((s, s2) -> {
				System.out.println("hello: " + s2);
			});
		}

		@Bean
		public java.util.function.Consumer<KStream<String, String>> anotherProcess() {
			return c -> c.foreach((s, s2) -> {
				System.out.println("s2s2s2::" + s2);
				countDownLatch2.countDown();
			});
		}
	}

	@EnableAutoConfiguration
	public static class FunctionCompositionConfig3 {

		@Bean
		public BiFunction<KStream<String, String>, KStream<String, String>, KStream<String, String>> fooBiFunc() {
			return KStream::merge;
		}

		@Bean
		public java.util.function.Consumer<KStream<String, String>> anotherProcess() {
			return c -> c.foreach((s, s2) -> {
				System.out.println("s2s2s2::" + s2);
				countDownLatch3.countDown();
			});
		}
	}

	@EnableAutoConfiguration
	public static class FunctionCompositionConfig4 {

		@Bean
		public BiFunction<KStream<String, String>, KTable<String, String>, KStream<String, String>> fooBiFunc() {
			return (a, b) -> a.join(b, (value1, value2) -> value1 + value2);
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> anotherFooFunc() {
			return input -> input.mapValues(value -> value + "From-anotherFooFunc");
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> yetAnotherFooFunc() {
			return input -> input.mapValues(value -> value + "From-yetAnotherFooFunc");
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> lastFunctionInChain() {
			return input -> input.mapValues(value -> value + "From-lastFunctionInChain");
		}
	}

	@EnableAutoConfiguration
	public static class FunctionCompositionConfig5 {

		@Bean
		public Function<KStream<String, String>, Function<KTable<String, String>, KStream<String, String>>> curriedFunc() {
			return a -> b ->
					a.join(b, (value1, value2) -> value1 + value2);
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> anotherFooFunc() {
			return input -> input.mapValues(value -> value + "From-anotherFooFunc");
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> yetAnotherFooFunc() {
			return input -> input.mapValues(value -> value + "From-yetAnotherFooFunc");
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> lastFunctionInChain() {
			return input -> input.mapValues(value -> value + "From-lastFunctionInChain");
		}
	}

	@EnableAutoConfiguration
	public static class FunctionCompositionConfig6 {

		@Bean
		public Function<KStream<String, String>, KTable<String, String>> fooFunc() {
			return ks -> {
				ks.foreach(new ForeachAction<String, String>() {
					@Override
					public void apply(String key, String value) {
						System.out.println();
					}
				});
				return ks.toTable();
			};
		}

		@Bean
		public Function<KTable<String, String>, KStream<String, String>> anotherProcess() {
			return KTable::toStream;
		}
	}
}
