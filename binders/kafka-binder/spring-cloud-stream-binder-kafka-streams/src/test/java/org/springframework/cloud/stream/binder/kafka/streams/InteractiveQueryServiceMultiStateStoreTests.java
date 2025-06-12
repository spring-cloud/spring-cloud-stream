/*
 * Copyright 2022-2024 the original author or authors.
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
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link InteractiveQueryService} when dealing with multiple KafkaStreams apps and state stores.
 *
 * @author Chris Bono
 * @author Soby Chacko
 */
@EmbeddedKafka(topics = {"input1", "input2"})
class InteractiveQueryServiceMultiStateStoreTests {

	private static final String STORE_1_NAME = "store1";

	private static final String STORE_2_NAME = "store2";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	void stateStoreAvailableOnProperAppWhenAppServerPropertySet() {
		stateStoreAvailableOnProperApp(true);
	}

	@Test
	void stateStoreAvailableOnProperAppWhenAppServerPropertyNotSet() {
		stateStoreAvailableOnProperApp(false);
	}

	private void stateStoreAvailableOnProperApp(boolean shouldSetAppServerProperty) {
		String appServerArg = shouldSetAppServerProperty ?
				"--spring.cloud.stream.kafka.streams.binder.configuration.application.server=" + embeddedKafka.getBrokersAsString() :
				"--foo=bar";
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder()
				.sources(MultipleAppsWithUsedStateStoresTestApplication.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0",
						"--spring.jmx.enabled=false",
						"--spring.cloud.function.definition=app1",
						"--spring.cloud.stream.function.bindings.app1-in-0=input1",
						"--spring.cloud.stream.kafka.streams.binder.functions.app1.application-id=stateStoreTestApp1",
						appServerArg,
						"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())
		) {
			waitForRunningStreams(context.getBean(KafkaStreamsRegistry.class));

			InteractiveQueryService queryService = context.getBean(InteractiveQueryService.class);

			// The KafkaStreams.store() used by query service is non-deterministic so perform the operation multiple times to
			// surface any possible issues. Also, no need to actually write anything to the stores, the store.get() call will
			// cause a failure when the state store is invalid.
			for (int i = 0; i < 100; i++) {
				assertThat(queryService.getQueryableStore(STORE_1_NAME, QueryableStoreTypes.keyValueStore())
						.get("someKey")).isNull();
			}
		}
	}

	@Test
	void stateStoreNotAvailableThrowsException() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder()
				.sources(MultipleAppsWithUnusedStateStoresTestApplication.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0",
						"--spring.jmx.enabled=false",
						"--spring.cloud.function.definition=app1;app2",
						"--spring.cloud.stream.function.bindings.app1-in-0=input1",
						"--spring.cloud.stream.function.bindings.app2-in-0=input2",
						"--spring.cloud.stream.kafka.streams.binder.functions.app1.application-id=stateStoreTestApp3",
						"--spring.cloud.stream.kafka.streams.binder.functions.app2.application-id=stateStoreTestApp4",
						"--spring.cloud.stream.kafka.streams.binder.configuration.application.server="
								+ embeddedKafka.getBrokersAsString(),
						"--spring.cloud.stream.kafka.streams.binder.brokers="
								+ embeddedKafka.getBrokersAsString())
		) {
			waitForRunningStreams(context.getBean(KafkaStreamsRegistry.class));

			InteractiveQueryService queryService = context.getBean(InteractiveQueryService.class);

			assertThatThrownBy(() -> queryService.getQueryableStore(STORE_1_NAME, QueryableStoreTypes.keyValueStore()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("Error retrieving state store: " + STORE_1_NAME)
					.hasRootCauseInstanceOf(UnknownStateStoreException.class)
					.hasRootCauseMessage("Store (" + STORE_1_NAME + ") not available to Streams instance");
			assertThatThrownBy(() -> queryService.getQueryableStore(STORE_2_NAME, QueryableStoreTypes.keyValueStore()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("Error retrieving state store: " + STORE_2_NAME)
					.hasRootCauseInstanceOf(UnknownStateStoreException.class)
					.hasRootCauseMessage("Store (" + STORE_2_NAME + ") not available to Streams instance");
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void storeQueryParameterCustomizerIsApplied() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder()
			.sources(StoreQueryParameterCustomizerTestApplication.class)
			.web(WebApplicationType.NONE)
			.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=app1",
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

		public static void main(String[] args) {
			SpringApplication.run(MultipleAppsWithUsedStateStoresTestApplication.class, args);
		}

		@Bean
		public StoreBuilder<KeyValueStore<String, String>> store1() {
			return Stores.keyValueStoreBuilder(
					Stores.persistentKeyValueStore(STORE_1_NAME), Serdes.String(), Serdes.String());
		}

		@Bean
		public Consumer<KStream<String, String>> app1() {
			return s -> s
					.processValues(EchoProcessor::new, STORE_1_NAME)
					.foreach((k, v) -> log.info("Echo {} -> {} into {}", k, v, STORE_1_NAME));
		}

		@Bean
		public StoreBuilder<KeyValueStore<String, String>> store2() {
			return Stores.keyValueStoreBuilder(
					Stores.persistentKeyValueStore(STORE_2_NAME), Serdes.String(), Serdes.String());
		}

		@Bean
		public Consumer<KStream<String, String>> app2() {
			return s -> s
					.processValues(EchoProcessor::new, STORE_1_NAME)
					.foreach((k, v) -> log.info("Echo {} -> {} into {}", k, v, STORE_2_NAME));
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(false, true);
		}

	}

	@EnableAutoConfiguration
	@Configuration(proxyBeanMethods = false)
	static class MultipleAppsWithUnusedStateStoresTestApplication {

		private static final Logger log = LoggerFactory.getLogger(MultipleAppsWithUnusedStateStoresTestApplication.class);

		public static void main(String[] args) {
			SpringApplication.run(MultipleAppsWithUnusedStateStoresTestApplication.class, args);
		}

		@Bean
		public StoreBuilder<KeyValueStore<String, String>> store1() {
			return Stores.keyValueStoreBuilder(
					Stores.persistentKeyValueStore(STORE_1_NAME), Serdes.String(), Serdes.String());
		}

		@Bean
		public Consumer<KStream<String, String>> app1() {
			// NOTE: No reference to the state store via transformer
			return s -> s
					.foreach((k, v) -> log.info("Echo {} -> {} into {}", k, v, STORE_1_NAME));
		}

		@Bean
		public StoreBuilder<KeyValueStore<String, String>> store2() {
			return Stores.keyValueStoreBuilder(
					Stores.persistentKeyValueStore(STORE_2_NAME), Serdes.String(), Serdes.String());
		}

		@Bean
		public Consumer<KStream<String, String>> app2() {
			// NOTE: No reference to the state store via transformer
			return s -> s
					.foreach((k, v) -> log.info("Echo {} -> {} into {}", k, v, STORE_2_NAME));
		}

		@Bean
		public CleanupConfig cleanupConfig() {
			return new CleanupConfig(false, true);
		}

	}

	@EnableAutoConfiguration
	@Configuration(proxyBeanMethods = false)
	static class StoreQueryParameterCustomizerTestApplication {

		private static final Logger log = LoggerFactory.getLogger(StoreQueryParameterCustomizerTestApplication.class);


		@Bean
		public StoreBuilder<KeyValueStore<String, String>> store1() {
			return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore(STORE_1_NAME), Serdes.String(), Serdes.String());
		}

		@Bean
		public Consumer<KStream<String, String>> app1() {
			return s -> s
				.processValues(EchoProcessor::new, STORE_1_NAME)
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

	static class EchoProcessor implements FixedKeyProcessor<String, String, String> {


		@Override
		public void process(FixedKeyRecord<String, String> fixedKeyRecord) {

		}

		@Override
		public void close() {
		}
	}
}
