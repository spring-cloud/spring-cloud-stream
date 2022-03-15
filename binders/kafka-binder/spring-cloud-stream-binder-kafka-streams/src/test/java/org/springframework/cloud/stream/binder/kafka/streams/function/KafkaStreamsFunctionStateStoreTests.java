/*
 * Copyright 2019-2022 the original author or authors.
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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka
public class KafkaStreamsFunctionStateStoreTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	@Test
	void testKafkaStreamsFuncionWithMultipleStateStores() throws Exception {
		SpringApplication app = new SpringApplication(StateStoreTestApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.definition=biConsumerBean;hello",
				"--spring.cloud.stream.bindings.biConsumerBean-in-0.destination=words",
				"--spring.cloud.stream.bindings.hello-in-0.destination=words",
				"--spring.cloud.stream.kafka.streams.binder.functions.changed.applicationId=testKafkaStreamsFuncionWithMultipleStateStores-123",
				"--spring.cloud.stream.kafka.streams.binder.functions.hello.applicationId=testKafkaStreamsFuncionWithMultipleStateStores-456",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate(context);
		}
	}

	private void receiveAndValidate(ConfigurableApplicationContext context) throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("words");
			template.sendDefault(1, "foobar");
			Thread.sleep(2000L);
			StateStoreTestApplication processorApplication = context
					.getBean(StateStoreTestApplication.class);

			KeyValueStore<Long, Long> state1 = processorApplication.state1;
			assertThat(processorApplication.processed1).isTrue();
			assertThat(state1 != null).isTrue();
			assertThat(state1.name()).isEqualTo("my-store");
			WindowStore<Long, Long> state2 = processorApplication.state2;
			assertThat(state2 != null).isTrue();
			assertThat(state2.name()).isEqualTo("other-store");
			assertThat(state2.persistent()).isTrue();

			KeyValueStore<Long, Long> state3 = processorApplication.state1;
			assertThat(processorApplication.processed2).isTrue();
			assertThat(state3 != null).isTrue();
			assertThat(state3.name()).isEqualTo("my-store");
			WindowStore<Long, Long> state4 = processorApplication.state2;
			assertThat(state4 != null).isTrue();
			assertThat(state4.name()).isEqualTo("other-store");
			assertThat(state4.persistent()).isTrue();
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	public static class StateStoreTestApplication {

		KeyValueStore<Long, Long> state1;
		WindowStore<Long, Long> state2;

		KeyValueStore<Long, Long> state3;
		WindowStore<Long, Long> state4;

		boolean processed1;
		boolean processed2;

		@Bean(name = "biConsumerBean")
		public java.util.function.BiConsumer<KStream<Object, String>, KStream<Object, String>> process() {
			return (input0, input1) ->
					input0.process((ProcessorSupplier<Object, String>) () -> new Processor<Object, String>() {
						@Override
						@SuppressWarnings("unchecked")
						public void init(ProcessorContext context) {
							state1 = (KeyValueStore<Long, Long>) context.getStateStore("my-store");
							state2 = (WindowStore<Long, Long>) context.getStateStore("other-store");
						}

						@Override
						public void process(Object key, String value) {
							processed1 = true;
						}

						@Override
						public void close() {

						}
					}, "my-store", "other-store");
		}

		@Bean
		public java.util.function.Consumer<KTable<Object, String>> hello() {
			return input -> {
				input.toStream().process(() -> new Processor<Object, String>() {
					@Override
					@SuppressWarnings("unchecked")
					public void init(ProcessorContext context) {
						state3 = (KeyValueStore<Long, Long>) context.getStateStore("my-store");
						state4 = (WindowStore<Long, Long>) context.getStateStore("other-store");
					}

					@Override
					public void process(Object key, String value) {
						processed2 = true;
					}

					@Override
					public void close() {

					}
				}, "my-store", "other-store");
			};
		}

		@Bean
		public StoreBuilder myStore() {
			return Stores.keyValueStoreBuilder(
					Stores.persistentKeyValueStore("my-store"), Serdes.Long(),
					Serdes.Long());
		}

		@Bean
		public StoreBuilder otherStore() {
			return Stores.windowStoreBuilder(
					Stores.persistentWindowStore("other-store",
							Duration.ofSeconds(3), Duration.ofSeconds(3),  false), Serdes.Long(),
					Serdes.Long());
		}
	}

}
