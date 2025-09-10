/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.SchemaResolver.SchemaResolverCustomizer;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;
import org.springframework.pulsar.support.header.ToStringPulsarHeaderMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PulsarBinderIntegrationTests}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@ExtendWith(OutputCaptureExtension.class)
@SuppressWarnings("JUnitMalformedDeclaration")
class PulsarBinderIntegrationTests implements PulsarTestContainerSupport {

	private static final int AWAIT_DURATION = 10;

	@Test
	void binderAndBindingPropsAreAppliedAndRespected(CapturedOutput output) {
		SpringApplication app = new SpringApplication(BinderAndBindingPropsTestConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
				"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
				"--spring.pulsar.producer.cache.enabled=false",
				"--spring.cloud.function.definition=textSupplier;textLogger",
				"--spring.cloud.stream.bindings.textLogger-in-0.destination=textSupplier-out-0",
				"--spring.pulsar.producer.name=textSupplierProducer-fromBase",
				"--spring.cloud.stream.pulsar.binder.producer.name=textSupplierProducer-fromBinder",
				"--spring.cloud.stream.pulsar.bindings.textSupplier-out-0.producer.name=textSupplierProducer-fromBinding",
				"--spring.cloud.stream.pulsar.binder.producer.max-pending-messages=1100",
				"--spring.cloud.stream.pulsar.binder.producer.block-if-queue-full=true",
				"--spring.cloud.stream.pulsar.binder.consumer.subscription.name=textLoggerSub-fromBinder",
				"--spring.cloud.stream.pulsar.binder.consumer.name=textLogger-fromBinder",
				"--spring.cloud.stream.pulsar.bindings.textLogger-in-0.consumer.name=textLogger-fromBinding")) {

			Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
					.until(() -> output.toString().contains("Hello binder: test-basic-scenario"));

			// now verify the properties were set onto producer and consumer as expected
			TrackingProducerFactory producerFactory = context.getBean(TrackingProducerFactory.class);
			assertThat(producerFactory.producersCreated).isNotEmpty().element(0)
					.hasFieldOrPropertyWithValue("producerName", "textSupplierProducer-fromBinding")
					.hasFieldOrPropertyWithValue("conf.maxPendingMessages", 1100)
					.hasFieldOrPropertyWithValue("conf.blockIfQueueFull", true);

			TrackingConsumerFactory consumerFactory = context.getBean(TrackingConsumerFactory.class);
			assertThat(consumerFactory.consumersCreated).isNotEmpty().element(0)
					.hasFieldOrPropertyWithValue("consumerName", "textLogger-fromBinding")
					.hasFieldOrPropertyWithValue("conf.subscriptionName", "textLoggerSub-fromBinder");
		}
	}

	@Nested
	class DefaultEncoding {

		@Test
		void primitiveTypeString(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveTextConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=textSupplier;textLogger",
					"--spring.cloud.stream.bindings.textLogger-in-0.destination=textSupplier-out-0",
					"--spring.cloud.stream.pulsar.bindings.textLogger-in-0.consumer.subscription.name=pbit-text-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: test-basic-scenario"));
			}
		}

		@Test
		void primitiveTypeFloat(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveFloatConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=piSupplier;piLogger",
					"--spring.cloud.stream.bindings.piSupplier-out-0.destination=pi-stream",
					"--spring.cloud.stream.bindings.piLogger-in-0.destination=pi-stream",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.subscription.name=pbit-float-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 3.14"));
			}
		}

	}

	@Nested
	class NativeEncoding {

		@Test
		void primitiveTypeFloat(CapturedOutput output) {
			SpringApplication app = new SpringApplication(PrimitiveFloatConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=piSupplier;piLogger",
					"--spring.cloud.stream.bindings.piLogger-in-0.destination=piSupplier-out-0",
					"--spring.cloud.stream.bindings.piSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.piSupplier-out-0.producer.schema-type=FLOAT",
					"--spring.cloud.stream.bindings.piLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.schema-type=FLOAT",
					"--spring.cloud.stream.pulsar.bindings.piLogger-in-0.consumer.subscription.name=pbit-float-sub2")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 3.14"));
			}
		}

		@Test
		void jsonTypeFooWithSchemaType(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-1",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-1",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.schema-type=JSON",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName(),
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.schema-type=JSON",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription.name=pbit-foo-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: Foo[value=5150]"));
			}
		}

		@Test
		void jsonTypeFooWithoutSchemaTypeDefaultsToJsonSchema(CapturedOutput output) {
			SpringApplication app = new SpringApplication(JsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=foo-stream-2",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=foo-stream-2",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName(),
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription.name=pbit-foo-sub2")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: Foo[value=5150]"));
			}
		}

		@Test
		void avroTypeUserWithSchemaType(CapturedOutput output) {
			SpringApplication app = new SpringApplication(AvroUserConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=userSupplier;userLogger",
					"--spring.cloud.stream.bindings.userSupplier-out-0.destination=user-stream-1",
					"--spring.cloud.stream.bindings.userLogger-in-0.destination=user-stream-1",
					"--spring.cloud.stream.bindings.userSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.schema-type=AVRO",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.bindings.userLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.schema-type=AVRO",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.subscription.name=pbit-user-sub1")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: User{name='user21', age=21}"));
			}
		}

		@Test
		void avroTypeUserWithoutSchemaTypeWithCustomMappingsViaProps(CapturedOutput output) {
			SpringApplication app = new SpringApplication(AvroUserConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=userSupplier;userLogger",
					"--spring.cloud.stream.bindings.userSupplier-out-0.destination=user-stream-2",
					"--spring.cloud.stream.bindings.userLogger-in-0.destination=user-stream-2",
					"--spring.cloud.stream.bindings.userSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.bindings.userLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.subscription.name=pbit-user-sub2",
					"--spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(User.class.getName()),
					"--spring.pulsar.defaults.type-mappings[0].schema-info.schema-type=AVRO")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: User{name='user21', age=21}"));
			}
		}

		@Test
		void avroTypeUserWithoutSchemaTypeWithCustomMappingsViaCustomizer(CapturedOutput output) {
			SpringApplication app = new SpringApplication(AvroUserConfigCustomMappings.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=userSupplier;userLogger",
					"--spring.cloud.stream.bindings.userSupplier-out-0.destination=user-stream-3",
					"--spring.cloud.stream.bindings.userLogger-in-0.destination=user-stream-3",
					"--spring.cloud.stream.bindings.userSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.bindings.userLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.subscription.name=pbit-user-sub3")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: User{name='user21', age=21}"));
			}
		}

		@Test
		void keyValueAvroTypeWithSchemaTypeAndCustomTypeMappingsViaProps(CapturedOutput output) {
			SpringApplication app = new SpringApplication(KeyValueAvroUserConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=userSupplier;userLogger",
					"--spring.cloud.stream.bindings.userSupplier-out-0.destination=kv-stream-1",
					"--spring.cloud.stream.bindings.userLogger-in-0.destination=kv-stream-1",
					"--spring.cloud.stream.bindings.userSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.schema-type=KEY_VALUE",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.bindings.userLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.schema-type=KEY_VALUE",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.subscription.name=pbit-kv-sub1",
					"--spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(User.class.getName()),
					"--spring.pulsar.defaults.type-mappings[0].schema-info.schema-type=AVRO")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 21->User{name='user21', age=21}"));
			}
		}

		@Test
		void keyValueAvroTypeWithoutSchemaTypeAndCustomTypeMappingsViaProps(CapturedOutput output) {
			SpringApplication app = new SpringApplication(KeyValueAvroUserConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=userSupplier;userLogger",
					"--spring.cloud.stream.bindings.userSupplier-out-0.destination=kv-stream-2",
					"--spring.cloud.stream.bindings.userLogger-in-0.destination=kv-stream-2",
					"--spring.cloud.stream.bindings.userSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.bindings.userLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.subscription.name=pbit-kv-sub2",
					"--spring.pulsar.defaults.type-mappings[0].message-type=%s".formatted(User.class.getName()),
					"--spring.pulsar.defaults.type-mappings[0].schema-info.schema-type=AVRO")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 21->User{name='user21', age=21}"));
			}
		}

		@Test
		void keyValueAvroTypeWithSchemaTypeAndCustomTypeMappingsViaCustomizer(CapturedOutput output) {
			SpringApplication app = new SpringApplication(KeyValueAvroUserConfigCustomMappings.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=userSupplier;userLogger",
					"--spring.cloud.stream.bindings.userSupplier-out-0.destination=kv-stream-3",
					"--spring.cloud.stream.bindings.userLogger-in-0.destination=kv-stream-3",
					"--spring.cloud.stream.bindings.userSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.schema-type=KEY_VALUE",
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userSupplier-out-0.producer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.bindings.userLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.schema-type=KEY_VALUE",
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-type="
							+ User.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.userLogger-in-0.consumer.subscription.name=pbit-kv-sub3")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 21->User{name='user21', age=21}"));
			}
		}

		@Test
		void keyValueJsonTypeWithoutSchemaTypeAndWithoutCustomTypeMappings(CapturedOutput output) {
			SpringApplication app = new SpringApplication(KeyValueJsonFooConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=fooSupplier;fooLogger",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.destination=kv-stream-4",
					"--spring.cloud.stream.bindings.fooLogger-in-0.destination=kv-stream-4",
					"--spring.cloud.stream.bindings.fooSupplier-out-0.producer.use-native-encoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-type="
							+ Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooSupplier-out-0.producer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.bindings.fooLogger-in-0.consumer.use-native-decoding=true",
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-type=" + Foo.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.message-key-type="
							+ String.class.getName(),
					"--spring.cloud.stream.pulsar.bindings.fooLogger-in-0.consumer.subscription.name=pbit-kv-sub4")) {
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: 5150->Foo[value=5150]"));
			}
		}

	}

	@Nested
	class CustomMessageHeaders {

		@Test
		void headersPropagatedSendAndReceive(CapturedOutput output) {
			SpringApplication app = new SpringApplication(CustomSimpleHeadersConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=springMessageSupplier;springMessageLogger",
					"--spring.cloud.stream.bindings.springMessageSupplier-out-0.destination=cmh-1",
					"--spring.cloud.stream.bindings.springMessageLogger-in-0.destination=cmh-1",
					"--spring.cloud.stream.pulsar.bindings.springMessageLogger-in-0.consumer.subscription.name=pbit-cmh1-sub1")) {
				// Wait for a few of the messages to flow through (check for index = 5)
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION)).until(
						() -> output.toString().contains("Hello binder: test-headers-msg-5 w/ custom-id: 5150-5"));
			}
		}

		@Test
		void complexHeadersAreEncodedAndPropagated(CapturedOutput output) {
			SpringApplication app = new SpringApplication(CustomComplexHeadersConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=springMessageSupplier;springMessageLogger",
					"--spring.cloud.stream.bindings.springMessageSupplier-out-0.destination=cmh-2",
					"--spring.cloud.stream.bindings.springMessageLogger-in-0.destination=cmh-2",
					"--spring.cloud.stream.pulsar.bindings.springMessageLogger-in-0.consumer.subscription.name=pbit-cmh2-sub1")) {
				// Wait for a few of the messages to flow through (check for index = 5)
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION)).until(() -> output.toString()
						.contains("Hello binder: test-headers-msg-5 w/ custom-id: FooHeader[value=5150-5]"));
			}
		}

		@Test
		void producerHeaderModeNone(CapturedOutput output) {
			SpringApplication app = new SpringApplication(CustomComplexHeadersConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=springMessageSupplier;springMessageLogger",
					"--spring.cloud.stream.bindings.springMessageSupplier-out-0.destination=cmh-3",
					"--spring.cloud.stream.bindings.springMessageSupplier-out-0.producer.header-mode=none",
					"--spring.cloud.stream.bindings.springMessageLogger-in-0.destination=cmh-3",
					"--spring.cloud.stream.pulsar.bindings.springMessageLogger-in-0.consumer.subscription.name=pbit-cmh3-sub1")) {
				// Wait for a few of the messages to flow through (check for index = 5)
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: test-headers-msg-5 w/ custom-id: null"));
			}
		}

		@Test
		void consumerHeaderModeNone(CapturedOutput output) {
			SpringApplication app = new SpringApplication(CustomComplexHeadersConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=springMessageSupplier;springMessageLogger",
					"--spring.cloud.stream.bindings.springMessageSupplier-out-0.destination=cmh-4",
					"--spring.cloud.stream.bindings.springMessageLogger-in-0.destination=cmh-4",
					"--spring.cloud.stream.bindings.springMessageLogger-in-0.consumer.header-mode=none",
					"--spring.cloud.stream.pulsar.bindings.springMessageLogger-in-0.consumer.subscription.name=pbit-cmh4-sub1")) {
				// Wait for a few of the messages to flow through (check for index = 5)
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION))
						.until(() -> output.toString().contains("Hello binder: test-headers-msg-5 w/ custom-id: null"));
			}
		}

		@Test
		void customHeaderMapperRespected(CapturedOutput output) {
			SpringApplication app = new SpringApplication(CustomHeaderMapperConfig.class);
			app.setWebApplicationType(WebApplicationType.NONE);
			try (ConfigurableApplicationContext ignored = app.run(
					"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
					"--spring.pulsar.admin.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
					"--spring.cloud.function.definition=springMessageSupplier;springMessageLogger",
					"--spring.cloud.stream.bindings.springMessageSupplier-out-0.destination=cmh-5",
					"--spring.cloud.stream.bindings.springMessageLogger-in-0.destination=cmh-5",
					"--spring.cloud.stream.pulsar.bindings.springMessageLogger-in-0.consumer.subscription.name=pbit-cmh5-sub1")) {
				// Wait for a few of the messages to flow through (check for index = 5)
				Awaitility.await().atMost(Duration.ofSeconds(AWAIT_DURATION)).until(() -> output.toString()
						.contains("Hello binder: test-headers-msg-5 w/ custom-id: tsh->tph->FooHeader[value=5150-5]"));
			}
		}

		@EnableAutoConfiguration
		@SpringBootConfiguration
		static class CustomSimpleHeadersConfig {

			private final Logger logger = LoggerFactory.getLogger(getClass());

			private int msgCount = 0;

			@Bean
			public Supplier<Message<String>> springMessageSupplier() {
				return () -> {
					msgCount++;
					return MessageBuilder.withPayload("test-headers-msg-" + msgCount)
							.setHeader("custom-id", "5150-" + msgCount).build();
				};
			}

			@Bean
			public Consumer<Message<String>> springMessageLogger() {
				return s -> this.logger.info("Hello binder: {} w/ custom-id: {}", s.getPayload(),
						s.getHeaders().get("custom-id"));
			}

		}

		@EnableAutoConfiguration
		@SpringBootConfiguration
		static class CustomComplexHeadersConfig {

			private final Logger logger = LoggerFactory.getLogger(getClass());

			private int msgCount = 0;

			@Bean
			public Supplier<Message<String>> springMessageSupplier() {
				return () -> {
					msgCount++;
					return MessageBuilder.withPayload("test-headers-msg-" + msgCount)
							.setHeader("custom-id", new FooHeader("5150-" + msgCount)).build();
				};
			}

			@Bean
			public Consumer<Message<String>> springMessageLogger() {
				return s -> {
					var header = s.getHeaders().get("custom-id");
					if (header != null) {
						assertThat(header).isInstanceOf(FooHeader.class);
					}
					this.logger.info("Hello binder: {} w/ custom-id: {}", s.getPayload(), header);
				};
			}

			record FooHeader(String value) {
			}

		}

		@EnableAutoConfiguration
		@SpringBootConfiguration
		static class CustomHeaderMapperConfig {

			private final Logger logger = LoggerFactory.getLogger(getClass());

			private int msgCount = 0;

			@Bean
			public PulsarHeaderMapper extendedToStringHeaderMapper() {
				return new ToStringPulsarHeaderMapper(List.of("custom-id"), List.of("foo", "custom-id")) {
					@Override
					public Map<String, String> toPulsarHeaders(MessageHeaders springHeaders) {
						Map<String, String> pulsarHeaders = super.toPulsarHeaders(springHeaders);
						// foo and custom-id are allowed and expected
						assertThat(pulsarHeaders).containsKeys("foo", "custom-id");
						return pulsarHeaders;
					}

					@Override
					public MessageHeaders toSpringHeaders(org.apache.pulsar.client.api.Message<?> pulsarMessage) {
						MessageHeaders springHeaders = super.toSpringHeaders(pulsarMessage);
						// foo not allowed, custom-id allowed
						assertThat(springHeaders).doesNotContainKey("foo").containsKey("custom-id");
						return springHeaders;
					}

					@Override
					protected String toPulsarHeaderValue(String name, Object value, Object context) {
						return "tph->" + super.toPulsarHeaderValue(name, value, context);
					}

					@Override
					protected Object toSpringHeaderValue(String headerName, String rawHeader, Object context) {
						return "tsh->" + super.toSpringHeaderValue(headerName, rawHeader, context);
					}
				};
			}

			@Bean
			public Supplier<Message<String>> springMessageSupplier() {
				return () -> {
					msgCount++;
					return MessageBuilder.withPayload("test-headers-msg-" + msgCount)
							.setHeader("foo", "bar-" + msgCount)
							.setHeader("custom-id", new FooHeader("5150-" + msgCount)).build();
				};
			}

			@Bean
			public Consumer<Message<String>> springMessageLogger() {
				return s -> {
					var header = s.getHeaders().get("custom-id");
					if (header != null) {
						assertThat(header).isInstanceOf(String.class);
					}
					var fooHeader = s.getHeaders().get("foo");
					assertThat(fooHeader).isNull();
					this.logger.info("Hello binder: {} w/ custom-id: {}", s.getPayload(), header);
				};
			}

			record FooHeader(String value) {
			}

		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class PrimitiveTextConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<String> textSupplier() {
			return () -> "test-basic-scenario";
		}

		@Bean
		public Consumer<String> textLogger() {
			return s -> this.logger.info("Hello binder: " + s);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(PrimitiveTextConfig.class)
	static class BinderAndBindingPropsTestConfig {

		@Bean
		TrackingProducerFactoryBeanPostProcessor trackingProducerFactory(PulsarClient pulsarClient) {
			return new TrackingProducerFactoryBeanPostProcessor(pulsarClient);
		}

		@Bean
		TrackingConsumerFactoryBeanPostProcessor trackingConsumerFactory() {
			return new TrackingConsumerFactoryBeanPostProcessor();
		}

	}

	static class TrackingProducerFactoryBeanPostProcessor implements BeanPostProcessor {

		private final PulsarClient pulsarClient;

		TrackingProducerFactoryBeanPostProcessor(PulsarClient pulsarClient) {
			this.pulsarClient = pulsarClient;
		}

		@Override
		public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
			if (bean instanceof DefaultPulsarProducerFactory defaultFactory) {
				return new TrackingProducerFactory(defaultFactory, this.pulsarClient);
			}
			return bean;
		}

	}

	static class TrackingProducerFactory implements PulsarProducerFactory<String> {

		private final DefaultPulsarProducerFactory<String> trackedProducerFactory;

		private final PulsarClient pulsarClient;

		List<Producer<String>> producersCreated = new ArrayList<>();

		TrackingProducerFactory(DefaultPulsarProducerFactory<String> trackedProducerFactory, PulsarClient pulsarClient) {
			this.trackedProducerFactory = trackedProducerFactory;
			this.pulsarClient = pulsarClient;
		}

		// This is required in PulsarProducerFactory in Spring Pulsar 1.1.x (i.e. Spring Boot 3.3.x)
		public PulsarClient getPulsarClient() {
			return this.pulsarClient;
		}

		@Override
		public Producer<String> createProducer(Schema<String> schema, String topic) {
			Producer<String> producer = null;
			try {
				producer = this.trackedProducerFactory.createProducer(schema, topic);
			}
			catch (Exception e) {
				// pass through
			}
			this.producersCreated.add(producer);
			return producer;
		}

		@Override
		public Producer<String> createProducer(Schema<String> schema, String topic,
				ProducerBuilderCustomizer<String> customizer) {
			Producer<String> producer = null;
			try {
				producer = this.trackedProducerFactory.createProducer(schema, topic, customizer);
			}
			catch (Exception e) {
				// pass through
			}
			this.producersCreated.add(producer);
			return producer;
		}

		@Override
		public Producer<String> createProducer(Schema<String> schema, String topic, Collection<String> encryptionKeys,
				List<ProducerBuilderCustomizer<String>> producerBuilderCustomizers) {
			Producer<String> producer = null;
			try {
				producer = this.trackedProducerFactory.createProducer(schema, topic, encryptionKeys,
						producerBuilderCustomizers);
			}
			catch (Exception e) {
				// pass through
			}
			this.producersCreated.add(producer);
			return producer;
		}

		@Override
		public String getDefaultTopic() {
			return this.trackedProducerFactory.getDefaultTopic();
		}

	}

	static class TrackingConsumerFactoryBeanPostProcessor implements BeanPostProcessor {

		@Override
		public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
			if (bean instanceof DefaultPulsarConsumerFactory defaultFactory) {
				return new TrackingConsumerFactory(defaultFactory);
			}
			return bean;
		}

	}

	static class TrackingConsumerFactory implements PulsarConsumerFactory<String> {

		private final DefaultPulsarConsumerFactory<String> trackedConsumerFactory;

		List<org.apache.pulsar.client.api.Consumer<String>> consumersCreated = new ArrayList<>();

		TrackingConsumerFactory(DefaultPulsarConsumerFactory<String> trackedConsumerFactory) {
			this.trackedConsumerFactory = trackedConsumerFactory;
		}

		@Override
		public org.apache.pulsar.client.api.Consumer<String> createConsumer(Schema<String> schema,
				Collection<String> topics, String subscriptionName, ConsumerBuilderCustomizer<String> customizer) {
			org.apache.pulsar.client.api.Consumer<String> consumer = null;
			try {
				consumer = this.trackedConsumerFactory.createConsumer(schema, topics, subscriptionName, customizer);
			}
			catch (Exception e) {
				// pass through
			}
			this.consumersCreated.add(consumer);
			return consumer;
		}

		@Override
		public org.apache.pulsar.client.api.Consumer<String> createConsumer(Schema<String> schema,
				Collection<String> topics, String subscriptionName, Map<String, String> metadataProperties,
				List<ConsumerBuilderCustomizer<String>> consumerBuilderCustomizers) {
			org.apache.pulsar.client.api.Consumer<String> consumer = null;
			try {
				consumer = this.trackedConsumerFactory.createConsumer(schema, topics, subscriptionName,
						metadataProperties, consumerBuilderCustomizers);
			}
			catch (Exception e) {
				// pass through
			}
			this.consumersCreated.add(consumer);
			return consumer;
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class PrimitiveFloatConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<Float> piSupplier() {
			return () -> 3.14f;
		}

		@Bean
		public Consumer<Float> piLogger() {
			return f -> this.logger.info("Hello binder: " + f);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class JsonFooConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<Foo> fooSupplier() {
			return () -> new Foo("5150");
		}

		@Bean
		public Consumer<Foo> fooLogger() {
			return f -> this.logger.info("Hello binder: " + f);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(JsonFooConfig.class)
	static class JsonFooWithCustomMappingConfig {

		@Bean
		public SchemaResolverCustomizer<DefaultSchemaResolver> customMappings() {
			return (resolver) -> resolver.addCustomSchemaMapping(Foo.class, JSONSchema.of(Foo.class));
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class AvroUserConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<User> userSupplier() {
			return () -> new User("user21", 21);
		}

		@Bean
		public Consumer<User> userLogger() {
			return f -> this.logger.info("Hello binder: " + f);
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(AvroUserConfig.class)
	static class AvroUserConfigCustomMappings {

		@Bean
		public SchemaResolverCustomizer<DefaultSchemaResolver> customMappings() {
			return (resolver) -> resolver.addCustomSchemaMapping(User.class, Schema.AVRO(User.class));
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class KeyValueAvroUserConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<KeyValue<String, User>> userSupplier() {
			return () -> new KeyValue<>("21", new User("user21", 21));
		}

		@Bean
		public Consumer<KeyValue<String, User>> userLogger() {
			return f -> this.logger.info("Hello binder: " + f.getKey() + "->" + f.getValue());
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(KeyValueAvroUserConfig.class)
	static class KeyValueAvroUserConfigCustomMappings {

		@Bean
		public SchemaResolverCustomizer<DefaultSchemaResolver> customMappings() {
			return (resolver) -> resolver.addCustomSchemaMapping(User.class, Schema.AVRO(User.class));
		}

	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class KeyValueJsonFooConfig {

		private final Logger logger = LoggerFactory.getLogger(getClass());

		@Bean
		public Supplier<KeyValue<String, Foo>> fooSupplier() {
			return () -> new KeyValue<>("5150", new Foo("5150"));
		}

		@Bean
		public Consumer<KeyValue<String, Foo>> fooLogger() {
			return f -> this.logger.info("Hello binder: " + f.getKey() + "->" + f.getValue());
		}

	}

	record Foo(String value) {
	}

	/**
	 * Do not convert this to a Record as Avro does not seem to work well w/ records.
	 */
	static class User {

		private String name;

		private int age;

		User() {
		}

		User(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			User user = (User) o;
			return age == user.age && Objects.equals(name, user.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age);
		}

		@Override
		public String toString() {
			return "User{" + "name='" + name + '\'' + ", age=" + age + '}';
		}

	}

}
