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

import java.lang.reflect.Method;
import java.util.Date;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KStream;
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
import org.springframework.util.Assert;

@EmbeddedKafka
public class SerdesProvidedAsBeansTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	@Test
	void testKstreamWordCountFunction() throws NoSuchMethodException {
		SpringApplication app = new SpringApplication(SerdeProvidedAsBeanApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=purchases",
				"--spring.cloud.stream.bindings.process-out-0.destination=coffee",
				"--spring.cloud.stream.kafka.streams.binder.functions.process.applicationId=process-id-0",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			final Method method = SerdeProvidedAsBeanApp.class.getMethod("process");

			ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, SerdeProvidedAsBeanApp.class);

			final KeyValueSerdeResolver keyValueSerdeResolver = context.getBean(KeyValueSerdeResolver.class);
			final BindingServiceProperties bindingServiceProperties = context.getBean(BindingServiceProperties.class);
			final KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties = context.getBean(KafkaStreamsExtendedBindingProperties.class);

			final ConsumerProperties consumerProperties = bindingServiceProperties.getBindingProperties("process-in-0").getConsumer();
			final KafkaStreamsConsumerProperties kafkaStreamsConsumerProperties = kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties("input");
			kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties("input");
			final Serde<?> inboundValueSerde = keyValueSerdeResolver.getInboundValueSerde(consumerProperties, kafkaStreamsConsumerProperties, resolvableType.getGeneric(0));

			Assert.isTrue(inboundValueSerde instanceof FooSerde, "Inbound Value Serde is not matched");

			final ProducerProperties producerProperties = bindingServiceProperties.getBindingProperties("process-out-0").getProducer();
			final KafkaStreamsProducerProperties kafkaStreamsProducerProperties = kafkaStreamsExtendedBindingProperties.getExtendedProducerProperties("output");
			kafkaStreamsExtendedBindingProperties.getExtendedProducerProperties("output");
			final Serde<?> outboundValueSerde = keyValueSerdeResolver.getOutboundValueSerde(producerProperties, kafkaStreamsProducerProperties, resolvableType.getGeneric(1));

			Assert.isTrue(outboundValueSerde instanceof FooSerde, "Outbound Value Serde is not matched");
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

	@EnableAutoConfiguration
	public static class SerdeProvidedAsBeanApp {

		@Bean
		public Function<KStream<String, Date>, KStream<String, Date>> process() {
			return input -> input;
		}

		@Bean
		public Serde<Date> fooSerde() {
			return new FooSerde<>();
		}
	}
}
