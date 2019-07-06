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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaStreamsFunctionStateStoreTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"counts");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Test
	public void testKafkaStreamsFuncionWithMultipleStateStores() throws Exception {
		SpringApplication app = new SpringApplication(StateStoreTestApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=words",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=basic-word-count-1",
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
			template.sendDefault("foobar");
			Thread.sleep(2000L);
			StateStoreTestApplication processorApplication = context
					.getBean(StateStoreTestApplication.class);

			KeyValueStore<Long, Long> state1 = processorApplication.state1;
			assertThat(processorApplication.processed).isTrue();
			assertThat(state1 != null).isTrue();
			assertThat(state1.name()).isEqualTo("my-store");
			WindowStore<Long, Long> state2 = processorApplication.state2;
			assertThat(state2 != null).isTrue();
			assertThat(state2.name()).isEqualTo("other-store");
			assertThat(state2.persistent()).isTrue();
		}
		finally {
			pf.destroy();
		}
	}

	@EnableBinding(KStreamProcessorX.class)
	@EnableAutoConfiguration
	static class StateStoreTestApplication {

		KeyValueStore<Long, Long> state1;
		WindowStore<Long, Long> state2;

		boolean processed;

		@Bean
		public java.util.function.Consumer<KStream<Object, String>> process() {
			return input ->
					input.process((ProcessorSupplier<Object, String>) () -> new Processor<Object, String>() {
						@Override
						@SuppressWarnings("unchecked")
						public void init(ProcessorContext context) {
							state1 = (KeyValueStore<Long, Long>) context.getStateStore("my-store");
							state2 = (WindowStore<Long, Long>) context.getStateStore("other-store");
						}

						@Override
						public void process(Object key, String value) {
							processed = true;
						}

						@Override
						public void close() {
							if (state1 != null) {
								state1.close();
							}
							if (state2 != null) {
								state2.close();
							}
						}
					}, "my-store", "other-store");
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
							3L, 3, 3L, false), Serdes.Long(),
					Serdes.Long());
		}
	}

	interface KStreamProcessorX  {
		@Input("input")
		KStream<?, ?> input();
	}

}
