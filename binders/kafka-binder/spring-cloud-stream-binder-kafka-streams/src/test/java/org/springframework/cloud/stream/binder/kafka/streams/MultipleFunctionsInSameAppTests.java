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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipleFunctionsInSameAppTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"coffee", "electronics");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	private static CountDownLatch countDownLatch = new CountDownLatch(2);

	@BeforeClass
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("purchase-groups", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "coffee", "electronics");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultiFunctionsInSameApp() throws InterruptedException {
		SpringApplication app = new SpringApplication(MultipleFunctionsInSameApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.definition=processItem;analyze;anotherProcess;yetAnotherProcess",
				"--spring.cloud.stream.bindings.processItem-in-0.destination=purchases",
				"--spring.cloud.stream.bindings.processItem-out-0.destination=coffee",
				"--spring.cloud.stream.bindings.processItem-out-1.destination=electronics",
				"--spring.cloud.stream.bindings.analyze-in-0.destination=coffee",
				"--spring.cloud.stream.bindings.analyze-in-1.destination=electronics",
				"--spring.cloud.stream.kafka.streams.binder.functions.analyze.applicationId=analyze-id-0",
				"--spring.cloud.stream.kafka.streams.binder.functions.processItem.applicationId=processItem-id-0",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.bindings.processItem-in-0.consumer.concurrency=2",
				"--spring.cloud.stream.bindings.analyze-in-0.consumer.concurrency=1",
				"--spring.cloud.stream.kafka.streams.binder.configuration.num.stream.threads=3",
				"--spring.cloud.stream.kafka.streams.binder.functions.processItem.configuration.client.id=processItem-client",
				"--spring.cloud.stream.kafka.streams.binder.functions.analyze.configuration.client.id=analyze-client",
				"--spring.cloud.stream.kafka.streams.binder.functions.anotherProcess.configuration.client.id=anotherProcess-client",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("purchases", "coffee", "electronics");

			StreamsBuilderFactoryBean processStreamsBuilderFactoryBean = context
					.getBean("&stream-builder-processItem", StreamsBuilderFactoryBean.class);

			StreamsBuilderFactoryBean analyzeStreamsBuilderFactoryBean = context
					.getBean("&stream-builder-analyze", StreamsBuilderFactoryBean.class);

			StreamsBuilderFactoryBean anotherProcessStreamsBuilderFactoryBean = context
					.getBean("&stream-builder-anotherProcess", StreamsBuilderFactoryBean.class);

			final Properties processStreamsConfiguration = processStreamsBuilderFactoryBean.getStreamsConfiguration();
			final Properties analyzeStreamsConfiguration = analyzeStreamsBuilderFactoryBean.getStreamsConfiguration();
			final Properties anotherProcessStreamsConfiguration = anotherProcessStreamsBuilderFactoryBean.getStreamsConfiguration();

			assertThat(processStreamsConfiguration.getProperty("client.id")).isEqualTo("processItem-client");
			assertThat(analyzeStreamsConfiguration.getProperty("client.id")).isEqualTo("analyze-client");

			Integer concurrency = (Integer) processStreamsConfiguration.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG);
			assertThat(concurrency).isEqualTo(2);
			concurrency = (Integer) analyzeStreamsConfiguration.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG);
			assertThat(concurrency).isEqualTo(1);
			assertThat(anotherProcessStreamsConfiguration.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG)).isEqualTo("3");

			final KafkaStreamsBindingInformationCatalogue catalogue = context.getBean(KafkaStreamsBindingInformationCatalogue.class);
			Field field = ReflectionUtils.findField(KafkaStreamsBindingInformationCatalogue.class, "outboundKStreamResolvables", Map.class);
			ReflectionUtils.makeAccessible(field);
			final Map<Object, ResolvableType> outboundKStreamResolvables = (Map<Object, ResolvableType>) ReflectionUtils.getField(field, catalogue);
			// Since we have 2 functions with return types -- one is an array return type with 2 bindings -- assert that
			// the catalogue contains outbound type information for all the 3 different bindings.
			assertThat(outboundKStreamResolvables.size()).isEqualTo(3);
		}
	}

	@Test
	public void testMultiFunctionsInSameAppWithMultiBinders() throws Exception {
		SpringApplication app = new SpringApplication(MultipleFunctionsInSameApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.definition=processItem;analyze",
				"--spring.cloud.stream.bindings.processItem-in-0.destination=purchases",
				"--spring.cloud.stream.kafka.streams.bindings.processItem-in-0.consumer.startOffset=latest",
				"--spring.cloud.stream.bindings.processItem-in-0.binder=kafka1",
				"--spring.cloud.stream.bindings.processItem-out-0.destination=coffee",
				"--spring.cloud.stream.bindings.processItem-out-0.binder=kafka1",
				"--spring.cloud.stream.bindings.processItem-out-1.destination=electronics",
				"--spring.cloud.stream.bindings.processItem-out-1.binder=kafka1",
				"--spring.cloud.stream.bindings.analyze-in-0.destination=coffee",
				"--spring.cloud.stream.bindings.analyze-in-0.binder=kafka2",
				"--spring.cloud.stream.bindings.analyze-in-1.destination=electronics",
				"--spring.cloud.stream.bindings.analyze-in-1.binder=kafka2",
				"--spring.cloud.stream.bindings.analyze-in-0.consumer.concurrency=2",
				"--spring.cloud.stream.binders.kafka1.type=kstream",
				"--spring.cloud.stream.binders.kafka1.environment.spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.binders.kafka1.environment.spring.cloud.stream.kafka.streams.binder.applicationId=my-app-1",
				"--spring.cloud.stream.binders.kafka1.environment.spring.cloud.stream.kafka.streams.binder.configuration.client.id=processItem-client",
				"--spring.cloud.stream.binders.kafka2.type=kstream",
				"--spring.cloud.stream.binders.kafka2.environment.spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.binders.kafka2.environment.spring.cloud.stream.kafka.streams.binder.applicationId=my-app-2",
				"--spring.cloud.stream.binders.kafka2.environment.spring.cloud.stream.kafka.streams.binder.configuration.client.id=analyze-client")) {

			Thread.sleep(1000);
			receiveAndValidate("purchases", "coffee", "electronics");

			StreamsBuilderFactoryBean processStreamsBuilderFactoryBean = context
					.getBean("&stream-builder-processItem", StreamsBuilderFactoryBean.class);

			StreamsBuilderFactoryBean analyzeStreamsBuilderFactoryBean = context
					.getBean("&stream-builder-analyze", StreamsBuilderFactoryBean.class);

			final Properties processStreamsConfiguration = processStreamsBuilderFactoryBean.getStreamsConfiguration();
			final Properties analyzeStreamsConfiguration = analyzeStreamsBuilderFactoryBean.getStreamsConfiguration();

			assertThat(processStreamsConfiguration.getProperty("application.id")).isEqualTo("my-app-1");
			assertThat(analyzeStreamsConfiguration.getProperty("application.id")).isEqualTo("my-app-2");
			assertThat(processStreamsConfiguration.getProperty("client.id")).isEqualTo("processItem-client");
			assertThat(analyzeStreamsConfiguration.getProperty("client.id")).isEqualTo("analyze-client");

			Integer concurrency = (Integer) analyzeStreamsConfiguration.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG);
			assertThat(concurrency).isEqualTo(2);

			concurrency = (Integer) processStreamsConfiguration.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG);
			assertThat(concurrency).isNull(); //thus default to 1 by Kafka Streams.
		}
	}

	private void receiveAndValidate(String in, String... out) throws InterruptedException {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic(in);
			template.sendDefault("coffee");
			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, out[0]);
			assertThat(cr.value().contains("coffee")).isTrue();

			template.sendDefault("electronics");
			cr = KafkaTestUtils.getSingleRecord(consumer, out[1]);
			assertThat(cr.value().contains("electronics")).isTrue();

			Assert.isTrue(countDownLatch.await(5, TimeUnit.SECONDS), "Analyze (BiConsumer) method didn't receive all the expected records");
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	public static class MultipleFunctionsInSameApp {

		@Bean
		public Function<KStream<String, String>, KStream<String, String>[]> processItem() {
			return input -> input.branch(
					(s, p) -> p.equalsIgnoreCase("coffee"),
					(s, p) -> p.equalsIgnoreCase("electronics"));
		}

		@Bean
		public Function<KStream<String, String>, KStream<String, Long>> yetAnotherProcess() {
			return input -> input.map((k, v) -> new KeyValue<>("foo", 1L));
		}

		@Bean
		public BiConsumer<KStream<String, String>, KStream<String, String>> analyze() {
			return (coffee, electronics) -> {
				coffee.foreach((s, p) -> countDownLatch.countDown());
				electronics.foreach((s, p) -> countDownLatch.countDown());
			};
		}

		@Bean
		public java.util.function.Consumer<KStream<String, String>> anotherProcess() {
			return c -> {

			};
		}
	}
}
