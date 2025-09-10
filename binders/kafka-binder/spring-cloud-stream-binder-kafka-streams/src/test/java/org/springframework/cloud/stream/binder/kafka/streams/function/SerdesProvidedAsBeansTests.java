/*
 * Copyright 2019-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Date;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.KeyValueSerdeResolver;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to verify proper resolution of {@link Serde serdes} when they are provided as beans.
 *
 * @author Soby Chako
 * @author Chris Bono
 */
@EmbeddedKafka(topics = { "topic1", "topic2" })
class SerdesProvidedAsBeansTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	void simpleSerdeBeansAreResolvedProperly() throws Exception {
		SpringApplication app = new SpringApplication(SerdesProvidedAsBeansTestApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
			"--server.port=0",
			"--spring.jmx.enabled=false",
			"--spring.cloud.function.definition=simpleProcess",
			"--spring.cloud.stream.bindings.simpleProcess-in-0.destination=topic1",
			"--spring.cloud.stream.bindings.simpleProcess-out-0.destination=topic2",
			"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
				"=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
				"=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Method method = SerdesProvidedAsBeansTestApp.class.getMethod("simpleProcess");
			ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, SerdesProvidedAsBeansTestApp.class);

			KeyValueSerdeResolver keyValueSerdeResolver = context.getBean(KeyValueSerdeResolver.class);

			BindingServiceProperties bindingServiceProperties = context.getBean(BindingServiceProperties.class);
			ConsumerProperties consumerProperties = bindingServiceProperties.getBindingProperties("simpleProcess-in-0").getConsumer();
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties = context.getBean(KafkaStreamsExtendedBindingProperties.class);
			KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties = kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties("input");
			Serde<?> inboundValueSerde = keyValueSerdeResolver.getInboundValueSerde(consumerProperties, kafkaStreamsConsumerProperties, resolvableType.getGeneric(0));

			assertThat(inboundValueSerde).isInstanceOf(FooSerde.class);

			ProducerProperties producerProperties = bindingServiceProperties.getBindingProperties("simpleProcess-out-0").getProducer();
			KafkaStreamsProducerProperties kafkaStreamsProducerProperties = kafkaStreamsExtendedBindingProperties.getExtendedProducerProperties("output");
			Serde<?> outboundValueSerde = keyValueSerdeResolver.getOutboundValueSerde(producerProperties, kafkaStreamsProducerProperties, resolvableType.getGeneric(1));

			assertThat(outboundValueSerde).isInstanceOf(FooSerde.class);
		}
	}

	@Test
	void genericSerdeBeansAreResolvedProperly() throws Exception {
		SpringApplication app = new SpringApplication(SerdesProvidedAsBeansTestApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
			"--server.port=0",
			"--spring.jmx.enabled=false",
			"--spring.cloud.function.definition=genericProcess",
			"--spring.cloud.stream.bindings.genericProcess-in-0.destination=topic1",
			"--spring.cloud.stream.bindings.genericProcess-out-0.destination=topic2",
			"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
				"=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
				"=org.apache.kafka.common.serialization.Serdes$StringSerde",
			"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Method method = SerdesProvidedAsBeansTestApp.class.getMethod("genericProcess");
			ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, SerdesProvidedAsBeansTestApp.class);

			KeyValueSerdeResolver keyValueSerdeResolver = context.getBean(KeyValueSerdeResolver.class);

			BindingServiceProperties bindingServiceProperties = context.getBean(BindingServiceProperties.class);
			ConsumerProperties consumerProperties = bindingServiceProperties.getBindingProperties("genericProcess-in-0").getConsumer();
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties = context.getBean(KafkaStreamsExtendedBindingProperties.class);
			KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties = kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties("input");
			Serde<?> inboundValueSerde = keyValueSerdeResolver.getInboundValueSerde(consumerProperties, kafkaStreamsConsumerProperties, resolvableType.getGeneric(0));

			assertThat(inboundValueSerde).isInstanceOf(GenericEventDateSerde.class);

			ProducerProperties producerProperties = bindingServiceProperties.getBindingProperties("genericProcess-out-0").getProducer();
			KafkaStreamsProducerProperties kafkaStreamsProducerProperties = kafkaStreamsExtendedBindingProperties.getExtendedProducerProperties("output");
			Serde<?> outboundValueSerde = keyValueSerdeResolver.getOutboundValueSerde(producerProperties, kafkaStreamsProducerProperties, resolvableType.getGeneric(1));

			assertThat(outboundValueSerde).isInstanceOf(GenericEventStringSerde.class);
		}
	}

	static class FooSerde<T> implements Serde<T> {
		@Override
		public Serializer<T> serializer() {
			return null;
		}

		@Override
		public Deserializer<T> deserializer() {
			return null;
		}
	}

	static class GenericEventDateSerde implements Serde<GenericEvent<Date>> {
		@Override
		public Serializer<GenericEvent<Date>> serializer() {
			return null;
		}

		@Override
		public Deserializer<GenericEvent<Date>> deserializer() {
			return null;
		}
	}

	static class GenericEventStringSerde implements Serde<GenericEvent<String>> {
		@Override
		public Serializer<GenericEvent<String>> serializer() {
			return null;
		}

		@Override
		public Deserializer<GenericEvent<String>> deserializer() {
			return null;
		}
	}

	static class GenericEvent<T> {

		static <X> GenericEvent<X> of(X newThing) {
			GenericEvent<X> newEvent = new GenericEvent<>();
			newEvent.setThing(newThing);
			return newEvent;
		}

		private T thing;

		public T getThing() {
			return thing;
		}

		public void setThing(T thing) {
			this.thing = thing;
		}
	}

	@EnableAutoConfiguration
	public static class SerdesProvidedAsBeansTestApp {

		@Bean
		public Serde<GenericEvent<Date>> genericEventDataSerde() {
			return new GenericEventDateSerde();
		}

		@Bean
		public Serde<GenericEvent<String>> genericEventStringSerde() {
			return new GenericEventStringSerde();
		}

		@Bean
		public Serde<Date> fooSerde() {
			return new FooSerde<>();
		}

		@Bean
		public Function<KStream<String, Date>, KStream<String, Date>> simpleProcess() {
			return input -> input;
		}

		@Bean
		public Function<KStream<String, GenericEvent<Date>>, KStream<String, GenericEvent<String>>> genericProcess() {
			return input -> input.map((k, v) -> new KeyValue(k, GenericEvent.of(v.toString())));
		}

	}
}
