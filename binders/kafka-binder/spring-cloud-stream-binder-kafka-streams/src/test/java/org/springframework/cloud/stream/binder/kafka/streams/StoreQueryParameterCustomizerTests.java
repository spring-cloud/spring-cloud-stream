/*
 * Copyright 2023-2023 the original author or authors.
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

import java.time.Duration;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EmbeddedKafka(topics = {"input1"})
public class StoreQueryParameterCustomizerTests {

	private static final String STORE_1_NAME = "store1";
	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	@Test
	@SuppressWarnings("unchecked")
	void storeQueryParameterCustomizerIsApplied() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder()
			.sources(MultipleAppsWithUsedStateStoresTestApplication.class)
			.web(WebApplicationType.NONE)
			.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.function.definition=app1",
				"--spring.cloud.stream.function.bindings.app1-in-0=input1",
				"--spring.cloud.stream.kafka.streams.binder.functions.app1.application-id=storeQueryParameterCustomizerIsAppliedAppId",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())
		) {
			waitForRunningStreams(context.getBean(KafkaStreamsRegistry.class));

			InteractiveQueryService queryService = context.getBean(InteractiveQueryService.class);

			StoreQueryParametersCustomizer<?> storeQueryParametersCustomizer = context.getBean(StoreQueryParametersCustomizer.class);
			StoreQueryParameters<?> storeQueryParams = StoreQueryParameters.fromNameAndType(STORE_1_NAME, QueryableStoreTypes.keyValueStore());

			when(storeQueryParametersCustomizer.customize(Mockito.any(StoreQueryParameters.class))).thenReturn(storeQueryParams);

			queryService.getQueryableStore(STORE_1_NAME, QueryableStoreTypes.keyValueStore()).get("someKey");
			verify(storeQueryParametersCustomizer).customize(Mockito.any(StoreQueryParameters.class));
		}
	}

	private void waitForRunningStreams(KafkaStreamsRegistry registry) {
		await().atMost(Duration.ofSeconds(60))
			.until(() -> registry.streamsBuilderFactoryBeans().stream()
				.allMatch(x -> x.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)));
	}

	@EnableAutoConfiguration
	@Configuration(proxyBeanMethods = false)
	static class MultipleAppsWithUsedStateStoresTestApplication {

		private static final Logger log = LoggerFactory.getLogger(MultipleAppsWithUsedStateStoresTestApplication.class);


		@Bean
		public StoreBuilder<KeyValueStore<String, String>> store1() {
			return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore(STORE_1_NAME), Serdes.String(), Serdes.String());
		}

		@Bean
		public Consumer<KStream<String, String>> app1() {
			return s -> s
				.transformValues(EchoTransformer::new, STORE_1_NAME)
				.foreach((k, v) -> log.info("Echo {} -> {} into {}", k, v, STORE_1_NAME));
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(false, true);
		}

		@Bean
		StoreQueryParametersCustomizer<?> storeQueryParametersCustomizer() {
			return Mockito.mock(StoreQueryParametersCustomizer.class);
		}

	}

	static class EchoTransformer implements ValueTransformerWithKey<String, String, String> {

		@Override
		public void init(ProcessorContext context) {
		}

		@Override
		public String transform(String key, String value) {
			return value;
		}

		@Override
		public void close() {
		}
	}

}
