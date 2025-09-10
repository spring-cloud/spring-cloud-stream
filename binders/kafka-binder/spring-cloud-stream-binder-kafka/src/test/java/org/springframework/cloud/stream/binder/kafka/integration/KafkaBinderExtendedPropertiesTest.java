/*
 * Copyright 2018-present the original author or authors.
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
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.KafkaBindingRebalanceListener;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 * @author Gary Russell
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.function.definition=process;processCustom",
		"spring.cloud.stream.function.bindings.process-in-0=standard-in",
		"spring.cloud.stream.function.bindings.process-out-0=standard-out",
		"spring.cloud.stream.function.bindings.processCustom-in-0=custom-in",
		"spring.cloud.stream.function.bindings.processCustom-out-0=custom-out",
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
@EmbeddedKafka
class KafkaBinderExtendedPropertiesTest {

	@Autowired
	private ConfigurableApplicationContext context;

	@Test
	void testDefiningNewBindingAndSettingItsProperties() throws Exception {
		BindingsLifecycleController controller = context.getBean(BindingsLifecycleController.class);
		KafkaConsumerProperties consumerProperties = controller.defineInputBinding("test-input-binding");
		boolean isAutoRebalanceEnabled = consumerProperties.isAutoRebalanceEnabled();
		assertThat(isAutoRebalanceEnabled).isTrue();
		consumerProperties.setAutoRebalanceEnabled(false);
		consumerProperties = controller.getExtensionProperties("test-input-binding");
		isAutoRebalanceEnabled = consumerProperties.isAutoRebalanceEnabled();
		assertThat(isAutoRebalanceEnabled).isFalse();
	}

	@Test
	void kafkaBinderExtendedProperties() throws Exception {

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

		RebalanceListener rebalanceListener = context.getBean(RebalanceListener.class);
		assertThat(rebalanceListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalanceListener.bindings.keySet()).contains("standard-in",
				"custom-in");
		assertThat(rebalanceListener.bindings.values()).contains(Boolean.TRUE,
				Boolean.TRUE);
	}

	@EnableAutoConfiguration
	@Configuration
	public static class KafkaMetricsTestConfig {

		@Bean
		public Function<String, String> process() {
			return payload -> payload;
		}

	@Bean
	public Function<String, String> processCustom() {
		return payload -> payload;
	}
		@Bean
		public RebalanceListener rebalanceListener() {
			return new RebalanceListener();
		}

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
