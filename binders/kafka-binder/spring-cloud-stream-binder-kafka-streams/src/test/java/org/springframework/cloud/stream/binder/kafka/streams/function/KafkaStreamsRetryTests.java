/*
 * Copyright 2020-2023 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.StreamRetryTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@EmbeddedKafka
class KafkaStreamsRetryTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private final static CountDownLatch LATCH1 = new CountDownLatch(2);
	private final static CountDownLatch LATCH2 = new CountDownLatch(4);

	@Test
	void retryTemplatePerBindingOnKStream() throws Exception {
		SpringApplication app = new SpringApplication(RetryTemplatePerConsumerBindingApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=process",
				"--spring.cloud.stream.bindings.process-in-0.destination=words",
				"--spring.cloud.stream.bindings.process-in-0.consumer.max-attempts=2",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testRetryTemplatePerBindingOnKStream",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			sendAndValidate(LATCH1);
		}
	}

	@Test
	void retryTemplateOnTableTypes() throws Exception {
		SpringApplication app = new SpringApplication(RetryTemplatePerConsumerBindingApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=tableTypes",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testRetryTemplateOnTableTypes",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			assertThat(context.getBean("tableTypes-in-0-RetryTemplate", RetryTemplate.class)).isNotNull();
			assertThat(context.getBean("tableTypes-in-1-RetryTemplate", RetryTemplate.class)).isNotNull();
		}
	}

	@Test
	void retryTemplateBeanProvidedByTheApp() throws Exception {
		SpringApplication app = new SpringApplication(CustomRetryTemplateApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=process",
				"--spring.cloud.stream.bindings.process-in-0.destination=words",
				"--spring.cloud.stream.bindings.process-in-0.consumer.retry-template-name=fooRetryTemplate",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testRetryTemplateBeanProvidedByTheApp",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			sendAndValidate(LATCH2);
			assertThatThrownBy(() -> context.getBean("process-in-0-RetryTemplate", RetryTemplate.class)).isInstanceOf(NoSuchBeanDefinitionException.class);
		}
	}

	private void sendAndValidate(CountDownLatch latch) throws InterruptedException {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("words");
			template.sendDefault("foobar");
			Assert.isTrue(latch.await(10, TimeUnit.SECONDS), "Foo");
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	public static class RetryTemplatePerConsumerBindingApp {

		@Bean
		public java.util.function.Consumer<KStream<Object, String>> process(@Lazy @Qualifier("process-in-0-RetryTemplate") RetryTemplate retryTemplate) {

			return input -> input
					.process(() -> new Processor<Object, String, Object, String>() {

						@Override
						public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Object, String> context) {
							Processor.super.init(context);
						}

						@Override
						public void process(Record<Object, String> record) {
							retryTemplate.execute(context -> {
								LATCH1.countDown();
								throw new RuntimeException();
							});
						}

						@Override
						public void close() {
						}
					});
		}

		@Bean
		public BiConsumer<KTable<?, ?>, GlobalKTable<?, ?>> tableTypes() {
			return (t, g) -> {
			};
		}
	}

	@EnableAutoConfiguration
	public static class CustomRetryTemplateApp {

		@Bean
		@StreamRetryTemplate
		RetryTemplate fooRetryTemplate() {
			RetryTemplate retryTemplate = new RetryTemplate();

			RetryPolicy retryPolicy = new SimpleRetryPolicy(4);
			FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
			backOffPolicy.setBackOffPeriod(1);

			retryTemplate.setBackOffPolicy(backOffPolicy);
			retryTemplate.setRetryPolicy(retryPolicy);

			return retryTemplate;
		}

		@Bean
		public java.util.function.Consumer<KStream<Object, String>> process() {

			return input -> input
					.process(() -> new Processor<Object, String, Object, String>() {

						@Override
						public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Object, String> context) {
							Processor.super.init(context);
						}

						@Override
						public void process(Record<Object, String> record) {
							fooRetryTemplate().execute(context -> {
								LATCH2.countDown();
								throw new RuntimeException();
							});
						}

						@Override
						public void close() {
						}
					});
		}
	}
}
