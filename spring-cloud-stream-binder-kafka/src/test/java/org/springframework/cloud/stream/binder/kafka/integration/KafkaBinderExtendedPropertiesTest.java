/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
properties = {"spring.cloud.stream.kafka.bindings.output.producer.configuration.key.serializer=FooSerializer.class",
		"spring.cloud.stream.kafka.default.producer.configuration.key.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.default.producer.configuration.value.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.bindings.input.consumer.configuration.key.serializer=FooSerializer.class",
		"spring.cloud.stream.kafka.default.consumer.configuration.key.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.default.consumer.configuration.value.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.default.producer.configuration.foo=bar",
		"spring.cloud.stream.kafka.bindings.output.producer.configuration.foo=bindingSpecificPropertyShouldWinOverDefault"})
public class KafkaBinderExtendedPropertiesTest {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";
	private static final String ZK_NODE_PROPERTY = "spring.cloud.stream.kafka.binder.zkNodes";

	@ClassRule
	public static EmbeddedKafkaRule kafkaEmbedded = new EmbeddedKafkaRule(1, true);

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY, kafkaEmbedded.getEmbeddedKafka().getBrokersAsString());
		System.setProperty(ZK_NODE_PROPERTY, kafkaEmbedded.getEmbeddedKafka().getZookeeperConnectionString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
		System.clearProperty(ZK_NODE_PROPERTY);
	}

	@Autowired
	private ConfigurableApplicationContext context;

	@Test
	public void testKafkaBinderExtendedProperties() {

		BinderFactory binderFactory = context.getBeanFactory().getBean(BinderFactory.class);
		Binder<MessageChannel, ? extends ConsumerProperties, ? extends ProducerProperties> kafkaBinder =
				binderFactory.getBinder("kafka", MessageChannel.class);

		KafkaProducerProperties kafkaProducerProperties =
				(KafkaProducerProperties)((ExtendedPropertiesBinder) kafkaBinder).getExtendedProducerProperties("output");

		//binding "output" gets FooSerializer defined on the binding itself and BarSerializer through default property.
		assertThat(kafkaProducerProperties.getConfiguration().get("key.serializer")).isEqualTo("FooSerializer.class");
		assertThat(kafkaProducerProperties.getConfiguration().get("value.serializer")).isEqualTo("BarSerializer.class");

		assertThat(kafkaProducerProperties.getConfiguration().get("foo")).isEqualTo("bindingSpecificPropertyShouldWinOverDefault");

		KafkaConsumerProperties kafkaConsumerProperties =
				(KafkaConsumerProperties)((ExtendedPropertiesBinder) kafkaBinder).getExtendedConsumerProperties("input");

		//binding "input" gets FooSerializer defined on the binding itself and BarSerializer through default property.
		assertThat(kafkaConsumerProperties.getConfiguration().get("key.serializer")).isEqualTo("FooSerializer.class");
		assertThat(kafkaConsumerProperties.getConfiguration().get("value.serializer")).isEqualTo("BarSerializer.class");

		KafkaProducerProperties customKafkaProducerProperties =
				(KafkaProducerProperties)((ExtendedPropertiesBinder) kafkaBinder).getExtendedProducerProperties("custom-out");

		//binding "output" gets BarSerializer and BarSerializer for ker.serializer/value.serializer through default properties.
		assertThat(customKafkaProducerProperties.getConfiguration().get("key.serializer")).isEqualTo("BarSerializer.class");
		assertThat(customKafkaProducerProperties.getConfiguration().get("value.serializer")).isEqualTo("BarSerializer.class");

		//through default properties.
		assertThat(customKafkaProducerProperties.getConfiguration().get("foo")).isEqualTo("bar");

		KafkaConsumerProperties customKafkaConsumerProperties =
				(KafkaConsumerProperties)((ExtendedPropertiesBinder) kafkaBinder).getExtendedConsumerProperties("custom-in");

		//binding "input" gets BarSerializer and BarSerializer for ker.serializer/value.serializer through default properties.
		assertThat(customKafkaConsumerProperties.getConfiguration().get("key.serializer")).isEqualTo("BarSerializer.class");
		assertThat(customKafkaConsumerProperties.getConfiguration().get("value.serializer")).isEqualTo("BarSerializer.class");
	}

	@EnableBinding(CustomBindingForExtendedPropertyTesting.class)
	@EnableAutoConfiguration
	public static class KafkaMetricsTestConfig {

		@StreamListener(Sink.INPUT)
		@SendTo(Processor.OUTPUT)
		public String process(String payload) throws InterruptedException {
			return payload;
		}

		@StreamListener("custom-in")
		@SendTo("custom-out")
		public String processCustom(String payload) throws InterruptedException {
			return payload;
		}

	}

	interface CustomBindingForExtendedPropertyTesting extends Processor{

		@Output("custom-out")
		MessageChannel customOut();

		@Input("custom-in")
		SubscribableChannel customIn();

	}

}
