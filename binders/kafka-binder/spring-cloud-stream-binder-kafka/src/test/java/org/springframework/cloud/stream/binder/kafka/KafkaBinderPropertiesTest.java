/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.common.AbstractKafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 */
@SpringBootTest(classes = { KafkaBinderConfiguration.class, KafkaAutoConfiguration.class,
		KafkaBinderPropertiesTest.class })
@TestPropertySource(locations = "classpath:binder-config.properties", properties =
	"spring.cloud.stream.kafka.binder.healthIndicatorConsumerGroup=health-consumer-group")
class KafkaBinderPropertiesTest {

	@Autowired
	private KafkaMessageChannelBinder kafkaMessageChannelBinder;

	@Autowired
	private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	@Autowired
	private KafkaBinderHealthIndicator kafkaBinderHealthIndicator;

	@Test
	@SuppressWarnings("unchecked")
	void kafkaBinderConfigurationProperties() throws Exception {
		assertThat(this.kafkaBinderConfigurationProperties).isNotNull();

		// Testing a scenario in health indicator that is originally triggered by a property in KafkaBinderConfigurationProperties,
		// which ultimately creates a Kafka Consumer in the health indicator implementation.
		assertThat(this.kafkaBinderConfigurationProperties.getHealthIndicatorConsumerGroup())
			.isEqualTo("health-consumer-group");
		assertThat(this.kafkaBinderHealthIndicator).isNotNull();
		Field consumerFactoryField = AbstractKafkaBinderHealthIndicator.class.getDeclaredField("consumerFactory");
		consumerFactoryField.setAccessible(true);
		ConsumerFactory<?, ?> healthIndicatorConsumerFactory =
			(ConsumerFactory<?, ?>) consumerFactoryField.get(this.kafkaBinderHealthIndicator);
		assertThat(healthIndicatorConsumerFactory).isNotNull();
		Consumer<?, ?> consumer = healthIndicatorConsumerFactory.createConsumer();
		ConsumerGroupMetadata consumerGroupMetadata = consumer.groupMetadata();
		assertThat(consumerGroupMetadata.groupId()).isEqualTo("health-consumer-group");

		KafkaProducerProperties kafkaProducerProperties = new KafkaProducerProperties();
		kafkaProducerProperties.setBufferSize(12345);
		kafkaProducerProperties.setBatchTimeout(100);
		kafkaProducerProperties.setCloseTimeout(10);
		kafkaProducerProperties
				.setCompressionType(KafkaProducerProperties.CompressionType.gzip);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = new ExtendedProducerProperties<>(
				kafkaProducerProperties);
		Method getProducerFactoryMethod = KafkaMessageChannelBinder.class
				.getDeclaredMethod("getProducerFactory", String.class,
						ExtendedProducerProperties.class, String.class, String.class);
		getProducerFactoryMethod.setAccessible(true);
		DefaultKafkaProducerFactory producerFactory = (DefaultKafkaProducerFactory) getProducerFactoryMethod
				.invoke(this.kafkaMessageChannelBinder, "bar", producerProperties, "bar.producer", "bar");
		Field producerFactoryConfigField = ReflectionUtils
				.findField(DefaultKafkaProducerFactory.class, "configs", Map.class);
		ReflectionUtils.makeAccessible(producerFactoryConfigField);
		Map<String, Object> producerConfigs = (Map<String, Object>) ReflectionUtils
				.getField(producerFactoryConfigField, producerFactory);
		assertThat(producerConfigs.get("batch.size")).isEqualTo("12345");
		assertThat(producerConfigs.get("linger.ms")).isEqualTo("100");
		assertThat(producerConfigs.get("key.serializer"))
				.isEqualTo(ByteArraySerializer.class);
		assertThat(producerConfigs.get("value.serializer"))
				.isEqualTo(ByteArraySerializer.class);
		assertThat(producerConfigs.get("compression.type")).isEqualTo("gzip");

		Field physicalCloseTimeoutField = ReflectionUtils
				.findField(DefaultKafkaProducerFactory.class, "physicalCloseTimeout", Duration.class);
		ReflectionUtils.makeAccessible(physicalCloseTimeoutField);
		Duration physicalCloseTimeoutConfig = (Duration) ReflectionUtils
				.getField(physicalCloseTimeoutField, producerFactory);
		assertThat(physicalCloseTimeoutConfig).isEqualTo(Duration.ofSeconds(10));

		List<String> bootstrapServers = new ArrayList<>();
		bootstrapServers.add("10.98.09.199:9082");
		assertThat((((String) producerConfigs.get("bootstrap.servers"))
				.contains("10.98.09.199:9082"))).isTrue();
		Method createKafkaConsumerFactoryMethod = KafkaMessageChannelBinder.class
				.getDeclaredMethod("createKafkaConsumerFactory", boolean.class,
						String.class, ExtendedConsumerProperties.class, String.class, String.class);
		createKafkaConsumerFactoryMethod.setAccessible(true);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(
				new KafkaConsumerProperties());
		DefaultKafkaConsumerFactory consumerFactory = (DefaultKafkaConsumerFactory) createKafkaConsumerFactoryMethod
				.invoke(this.kafkaMessageChannelBinder, true, "test", consumerProperties, "test.consumer", "test");
		Field consumerFactoryConfigField = ReflectionUtils
				.findField(DefaultKafkaConsumerFactory.class, "configs", Map.class);
		ReflectionUtils.makeAccessible(consumerFactoryConfigField);
		Map<String, Object> consumerConfigs = (Map<String, Object>) ReflectionUtils
				.getField(consumerFactoryConfigField, consumerFactory);
		assertThat(consumerConfigs.get("key.deserializer"))
				.isEqualTo(ByteArrayDeserializer.class);
		assertThat(consumerConfigs.get("value.deserializer"))
				.isEqualTo(ByteArrayDeserializer.class);
		assertThat((((String) consumerConfigs.get("bootstrap.servers"))
				.contains("10.98.09.199:9082"))).isTrue();
	}

}
