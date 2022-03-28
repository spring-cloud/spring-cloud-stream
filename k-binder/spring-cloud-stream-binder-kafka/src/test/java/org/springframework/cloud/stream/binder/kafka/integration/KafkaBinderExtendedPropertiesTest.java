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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
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
import org.springframework.cloud.stream.binder.kafka.KafkaBindingRebalanceListener;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 * @author Gary Russell
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.stream.kafka.bindings.standard-out.producer.configuration.key.serializer=FooSerializer.class",
		"spring.cloud.stream.kafka.default.producer.configuration.key.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.default.producer.configuration.value.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.bindings.standard-in.consumer.configuration.key.serializer=FooSerializer.class",
		"spring.cloud.stream.kafka.default.consumer.configuration.key.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.default.consumer.configuration.value.serializer=BarSerializer.class",
		"spring.cloud.stream.kafka.default.producer.configuration.foo=bar",
		"spring.cloud.stream.kafka.bindings.standard-out.producer.configuration.foo="
				+ "bindingSpecificPropertyShouldWinOverDefault",
		"spring.cloud.stream.kafka.default.consumer.ackEachRecord=true",
		"spring.cloud.stream.kafka.bindings.custom-in.consumer.ackEachRecord=false" })
@DirtiesContext
public class KafkaBinderExtendedPropertiesTest {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	@ClassRule
	public static EmbeddedKafkaRule kafkaEmbedded = new EmbeddedKafkaRule(1, true);

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				kafkaEmbedded.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@Autowired
	private ConfigurableApplicationContext context;

	@Test
	public void testKafkaBinderExtendedProperties() throws Exception {

		BinderFactory binderFactory = context.getBeanFactory()
				.getBean(BinderFactory.class);
		Binder<MessageChannel, ? extends ConsumerProperties, ? extends ProducerProperties> kafkaBinder = binderFactory
				.getBinder("kafka", MessageChannel.class);

		KafkaProducerProperties kafkaProducerProperties = (KafkaProducerProperties) ((ExtendedPropertiesBinder) kafkaBinder)
				.getExtendedProducerProperties("standard-out");

		// binding "standard-out" gets FooSerializer defined on the binding itself
		// and BarSerializer through default property.
		assertThat(kafkaProducerProperties.getConfiguration().get("key.serializer"))
				.isEqualTo("FooSerializer.class");
		assertThat(kafkaProducerProperties.getConfiguration().get("value.serializer"))
				.isEqualTo("BarSerializer.class");

		assertThat(kafkaProducerProperties.getConfiguration().get("foo"))
				.isEqualTo("bindingSpecificPropertyShouldWinOverDefault");

		// @checkstyle:off
		KafkaConsumerProperties kafkaConsumerProperties = (KafkaConsumerProperties) ((ExtendedPropertiesBinder) kafkaBinder)
				.getExtendedConsumerProperties("standard-in");
		// @checkstyle:on
		// binding "standard-in" gets FooSerializer defined on the binding itself
		// and BarSerializer through default property.
		assertThat(kafkaConsumerProperties.getConfiguration().get("key.serializer"))
				.isEqualTo("FooSerializer.class");
		assertThat(kafkaConsumerProperties.getConfiguration().get("value.serializer"))
				.isEqualTo("BarSerializer.class");

		// @checkstyle:off
		KafkaProducerProperties customKafkaProducerProperties = (KafkaProducerProperties) ((ExtendedPropertiesBinder) kafkaBinder)
				.getExtendedProducerProperties("custom-out");
		// @checkstyle:on

		// binding "standard-out" gets BarSerializer and BarSerializer for
		// key.serializer/value.serializer through default properties.
		assertThat(customKafkaProducerProperties.getConfiguration().get("key.serializer"))
				.isEqualTo("BarSerializer.class");
		assertThat(
				customKafkaProducerProperties.getConfiguration().get("value.serializer"))
						.isEqualTo("BarSerializer.class");

		// through default properties.
		assertThat(customKafkaProducerProperties.getConfiguration().get("foo"))
				.isEqualTo("bar");

		// @checkstyle:off
		KafkaConsumerProperties customKafkaConsumerProperties = (KafkaConsumerProperties) ((ExtendedPropertiesBinder) kafkaBinder)
				.getExtendedConsumerProperties("custom-in");
		// @checkstyle:on
		// binding "standard-in" gets BarSerializer and BarSerializer for
		// key.serializer/value.serializer through default properties.
		assertThat(customKafkaConsumerProperties.getConfiguration().get("key.serializer"))
				.isEqualTo("BarSerializer.class");
		assertThat(
				customKafkaConsumerProperties.getConfiguration().get("value.serializer"))
						.isEqualTo("BarSerializer.class");

		assertThat(kafkaConsumerProperties.isAckEachRecord()).isEqualTo(true);
		assertThat(customKafkaConsumerProperties.isAckEachRecord()).isEqualTo(false);

		RebalanceListener rebalanceListener = context.getBean(RebalanceListener.class);
		assertThat(rebalanceListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalanceListener.bindings.keySet()).contains("standard-in",
				"custom-in");
		assertThat(rebalanceListener.bindings.values()).containsExactly(Boolean.TRUE,
				Boolean.TRUE);
	}

	@EnableBinding(CustomBindingForExtendedPropertyTesting.class)
	@EnableAutoConfiguration
	public static class KafkaMetricsTestConfig {

		@StreamListener("standard-in")
		@SendTo("standard-out")
		public String process(String payload) {
			return payload;
		}

		@StreamListener("custom-in")
		@SendTo("custom-out")
		public String processCustom(String payload) {
			return payload;
		}

		@Bean
		public RebalanceListener rebalanceListener() {
			return new RebalanceListener();
		}

	}

	interface CustomBindingForExtendedPropertyTesting {

		@Input("standard-in")
		SubscribableChannel standardIn();

		@Output("standard-out")
		MessageChannel standardOut();

		@Input("custom-in")
		SubscribableChannel customIn();

		@Output("custom-out")
		MessageChannel customOut();

	}

	public static class RebalanceListener implements KafkaBindingRebalanceListener {

		private final Map<String, Boolean> bindings = new HashMap<>();

		private final CountDownLatch latch = new CountDownLatch(2);

		@Override
		public void onPartitionsRevokedBeforeCommit(String bindingName,
				Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

		}

		@Override
		public void onPartitionsRevokedAfterCommit(String bindingName,
				Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

		}

		@Override
		public void onPartitionsAssigned(String bindingName, Consumer<?, ?> consumer,
				Collection<TopicPartition> partitions, boolean initial) {

			this.bindings.put(bindingName, initial);
			this.latch.countDown();
		}

	}

}
