/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Lei Chen
 * @author Soby Chacko
 */
public class KafkaStreamsStateStoreIntegrationTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"counts-id");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	@Test
	public void testKstreamStateStore() throws Exception {
		SpringApplication app = new SpringApplication(ProductCountApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.bindings.process-in-0=input",
				"--spring.cloud.stream.bindings.input.destination=foobar",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.applicationId"
						+ "=KafkaStreamsStateStoreIntegrationTests-abc",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
		try {
			Thread.sleep(2000);
			receiveAndValidateFoo(context, ProductCountApplication.class);
		}
		catch (Exception e) {
			throw e;
		}
		finally {
			context.close();
		}
	}

	@Test
	public void testSameStateStoreIsCreatedOnlyOnceWhenMultipleInputBindingsArePresent() throws Exception {
		SpringApplication app = new SpringApplication(ProductCountApplicationWithMultipleInputBindings.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.bindings.process-in-0=input1",
				"--spring.cloud.stream.function.bindings.process-in-1=input2",
				"--spring.cloud.stream.bindings.input1.destination=foobar",
				"--spring.cloud.stream.bindings.input2.destination=hello-foobar",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.input1.consumer.applicationId"
						+ "=KafkaStreamsStateStoreIntegrationTests-abc",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString());
		try {
			Thread.sleep(2000);
			// We are not particularly interested in querying the state store here, as that is verified by the other test
			// in this class. This test verifies that the same store is not attempted to be created by multiple input bindings.
			// Normally, that will cause an exception to be thrown. However by not getting any exceptions, we are verifying
			// that the binder is handling it appropriately.
			//For more info, see this issue: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/551
		}
		catch (Exception e) {
			throw e;
		}
		finally {
			context.close();
		}
	}

	private void receiveAndValidateFoo(ConfigurableApplicationContext context, Class<?> clazz)
			throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
				senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("foobar");
		template.sendDefault("{\"id\":\"123\"}");
		Thread.sleep(1000);

		// assertions
		if (clazz.isAssignableFrom(ProductCountApplication.class)) {
			ProductCountApplication productCount = context
					.getBean(ProductCountApplication.class);
			WindowStore<Object, String> state = productCount.state;
			assertThat(state != null).isTrue();
			assertThat(state.name()).isEqualTo("mystate");
			assertThat(state.persistent()).isTrue();
			assertThat(productCount.processed).isTrue();
		}
		else {
			fail("Expected assertions did not happen");
		}

	}

	@EnableAutoConfiguration
	public static class ProductCountApplication {

		WindowStore<Object, String> state;

		boolean processed;

		@Bean
		public Consumer<KStream<Object, Product>> process() {

			return input -> input.process(() -> new Processor<Object, Product>() {

				@Override
				public void init(ProcessorContext processorContext) {
					state = (WindowStore) processorContext.getStateStore("mystate");
				}

				@Override
				public void process(Object s, Product product) {
					processed = true;
				}

				@Override
				public void close() {
					if (state != null) {
						state.close();
					}
				}
			}, "mystate");
		}

		@Bean
		public StoreBuilder mystore() {
			return Stores.windowStoreBuilder(
					Stores.persistentWindowStore("mystate",
							Duration.ofMillis(3), Duration.ofMillis(3), false), Serdes.String(),
					Serdes.String());
		}
	}

	@EnableAutoConfiguration
	public static class ProductCountApplicationWithMultipleInputBindings {

		WindowStore<Object, String> state;

		boolean processed;

		@Bean
		public BiConsumer<KStream<Object, Product>, KStream<Object, Product>> process() {

			return (input, input2) -> {

				input.process(() -> new Processor<Object, Product>() {

					@Override
					public void init(ProcessorContext processorContext) {
						state = (WindowStore) processorContext.getStateStore("mystate");
					}

					@Override
					public void process(Object s, Product product) {
						processed = true;
					}

					@Override
					public void close() {
						if (state != null) {
							state.close();
						}
					}
				}, "mystate");
				//simple use of input2, we are not using input2 for anything other than triggering some test behavior.
				input2.foreach((key, value) -> { });
			};
		}

		@Bean
		public StoreBuilder mystore() {
			return Stores.windowStoreBuilder(
					Stores.persistentWindowStore("mystate",
							Duration.ofMillis(3), Duration.ofMillis(3), false), Serdes.String(),
					Serdes.String());
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
