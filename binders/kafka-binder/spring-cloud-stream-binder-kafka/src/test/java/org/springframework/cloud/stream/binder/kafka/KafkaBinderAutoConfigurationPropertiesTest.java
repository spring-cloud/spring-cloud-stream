/*
 * Copyright 2016-2022 the original author or authors.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { KafkaBinderConfiguration.class })
@TestPropertySource(locations = "classpath:binder-config-autoconfig.properties")
public class KafkaBinderAutoConfigurationPropertiesTest {

	@Autowired
	private KafkaMessageChannelBinder kafkaMessageChannelBinder;

	@Autowired
	private KafkaBinderHealthIndicator kafkaBinderHealthIndicator;

	@Test
	@SuppressWarnings("unchecked")
	void testKafkaBinderConfigurationWithKafkaProperties() throws Exception {
		assertThat(this.kafkaMessageChannelBinder).isNotNull();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());
		Method getProducerFactoryMethod = KafkaMessageChannelBinder.class
				.getDeclaredMethod("getProducerFactory", String.class,
						ExtendedProducerProperties.class, String.class, String.class);
		getProducerFactoryMethod.setAccessible(true);
		DefaultKafkaProducerFactory producerFactory = (DefaultKafkaProducerFactory) getProducerFactoryMethod
				.invoke(this.kafkaMessageChannelBinder, "foo", producerProperties, "foo.producer", "foo");
		Field producerFactoryConfigField = ReflectionUtils
				.findField(DefaultKafkaProducerFactory.class, "configs", Map.class);
		ReflectionUtils.makeAccessible(producerFactoryConfigField);
		Map<String, Object> producerConfigs = (Map<String, Object>) ReflectionUtils
				.getField(producerFactoryConfigField, producerFactory);
		assertThat(producerConfigs.get("batch.size").equals(10)).isTrue();
		assertThat(producerConfigs.get("key.serializer")).isEqualTo(LongSerializer.class);
		assertThat(producerConfigs.get("key.deserializer")).isNull();
		assertThat(producerConfigs.get("value.serializer"))
				.isEqualTo(LongSerializer.class);
		assertThat(producerConfigs.get("value.deserializer")).isNull();
		assertThat(producerConfigs.get("compression.type")).isEqualTo("snappy");
		List<String> bootstrapServers = new ArrayList<>();
		bootstrapServers.add("10.98.09.199:9092");
		bootstrapServers.add("10.98.09.196:9092");
		assertThat((((List<String>) producerConfigs.get("bootstrap.servers"))
				.containsAll(bootstrapServers))).isTrue();
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
				.isEqualTo(LongDeserializer.class);
		assertThat(consumerConfigs.get("key.serializer")).isNull();
		assertThat(consumerConfigs.get("value.deserializer"))
				.isEqualTo(LongDeserializer.class);
		assertThat(consumerConfigs.get("value.serialized")).isNull();
		assertThat(consumerConfigs.get("group.id")).isEqualTo("test");
		assertThat(consumerConfigs.get("auto.offset.reset")).isEqualTo("earliest");
		assertThat((((List<String>) consumerConfigs.get("bootstrap.servers"))
				.containsAll(bootstrapServers))).isTrue();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testKafkaHealthIndicatorProperties() {
		assertThat(this.kafkaBinderHealthIndicator).isNotNull();
		Field consumerFactoryField = ReflectionUtils.findField(
				KafkaBinderHealthIndicator.class, "consumerFactory",
				ConsumerFactory.class);
		ReflectionUtils.makeAccessible(consumerFactoryField);
		DefaultKafkaConsumerFactory consumerFactory = (DefaultKafkaConsumerFactory) ReflectionUtils
				.getField(consumerFactoryField, this.kafkaBinderHealthIndicator);
		Field configField = ReflectionUtils.findField(DefaultKafkaConsumerFactory.class,
				"configs", Map.class);
		ReflectionUtils.makeAccessible(configField);
		Map<String, Object> configs = (Map<String, Object>) ReflectionUtils
				.getField(configField, consumerFactory);
		assertThat(configs.containsKey("bootstrap.servers")).isTrue();
		List<String> bootstrapServers = new ArrayList<>();
		bootstrapServers.add("10.98.09.199:9092");
		bootstrapServers.add("10.98.09.196:9092");
		assertThat(((List<String>) configs.get("bootstrap.servers"))
				.containsAll(bootstrapServers)).isTrue();
	}

}
