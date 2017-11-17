/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { KafkaBinderConfiguration.class, KafkaAutoConfiguration.class,
		KafkaBinderConfigurationPropertiesTest.class })
@TestPropertySource(locations = "classpath:binder-config.properties")
public class KafkaBinderConfigurationPropertiesTest {

	@Autowired
	private KafkaMessageChannelBinder kafkaMessageChannelBinder;

	@Test
	public void testKafkaBinderConfigurationProperties() throws Exception {
		assertNotNull(this.kafkaMessageChannelBinder);
		KafkaProducerProperties kafkaProducerProperties = new KafkaProducerProperties();
		kafkaProducerProperties.setBufferSize(12345);
		kafkaProducerProperties.setBatchTimeout(100);
		kafkaProducerProperties.setCompressionType(KafkaProducerProperties.CompressionType.gzip);
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = new ExtendedProducerProperties<>(
				kafkaProducerProperties);
		Method getProducerFactoryMethod = KafkaMessageChannelBinder.class.getDeclaredMethod("getProducerFactory",
				String.class, ExtendedProducerProperties.class);
		getProducerFactoryMethod.setAccessible(true);
		DefaultKafkaProducerFactory producerFactory = (DefaultKafkaProducerFactory) getProducerFactoryMethod
				.invoke(this.kafkaMessageChannelBinder, "bar", producerProperties);
		Field producerFactoryConfigField = ReflectionUtils.findField(DefaultKafkaProducerFactory.class, "configs",
				Map.class);
		ReflectionUtils.makeAccessible(producerFactoryConfigField);
		Map<String, Object> producerConfigs = (Map<String, Object>) ReflectionUtils.getField(producerFactoryConfigField,
				producerFactory);
		assertTrue(producerConfigs.get("batch.size").equals("12345"));
		assertTrue(producerConfigs.get("linger.ms").equals("100"));
		assertTrue(producerConfigs.get("key.serializer").equals(ByteArraySerializer.class));
		assertTrue(producerConfigs.get("value.serializer").equals(ByteArraySerializer.class));
		assertTrue(producerConfigs.get("compression.type").equals("gzip"));
		List<String> bootstrapServers = new ArrayList<>();
		bootstrapServers.add("10.98.09.199:9082");
		assertTrue((((String) producerConfigs.get("bootstrap.servers")).contains("10.98.09.199:9082")));
		Method createKafkaConsumerFactoryMethod = KafkaMessageChannelBinder.class.getDeclaredMethod(
				"createKafkaConsumerFactory", boolean.class, String.class, ExtendedConsumerProperties.class);
		createKafkaConsumerFactoryMethod.setAccessible(true);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(
				new KafkaConsumerProperties());
		DefaultKafkaConsumerFactory consumerFactory = (DefaultKafkaConsumerFactory) createKafkaConsumerFactoryMethod
				.invoke(this.kafkaMessageChannelBinder, true, "test", consumerProperties);
		Field consumerFactoryConfigField = ReflectionUtils.findField(DefaultKafkaConsumerFactory.class, "configs",
				Map.class);
		ReflectionUtils.makeAccessible(consumerFactoryConfigField);
		Map<String, Object> consumerConfigs = (Map<String, Object>) ReflectionUtils.getField(consumerFactoryConfigField,
				consumerFactory);
		assertTrue(consumerConfigs.get("key.deserializer").equals(ByteArrayDeserializer.class));
		assertTrue(consumerConfigs.get("value.deserializer").equals(ByteArrayDeserializer.class));
		assertTrue((((String) consumerConfigs.get("bootstrap.servers")).contains("10.98.09.199:9082")));
	}

}
