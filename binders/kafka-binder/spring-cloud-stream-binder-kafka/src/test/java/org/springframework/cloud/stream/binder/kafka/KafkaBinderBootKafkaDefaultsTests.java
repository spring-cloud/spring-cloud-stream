/*
 * Copyright 2016-present the original author or authors.
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

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.utils.BindingUtils;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Having the Kafka binder on the classpath must not change the {@code spring.kafka}
 * (de)serializer defaults that Spring Boot applies to other Kafka users, such as
 * {@code @KafkaListener}, while the binder itself keeps working with {@code byte[]}.
 *
 * @author Oleg Zhurakousky
 */
@SpringBootTest(classes = KafkaBinderConfiguration.class, properties = "spring.kafka.bootstrap-servers=localhost:9092")
class KafkaBinderBootKafkaDefaultsTests {

	@Autowired
	private Environment environment;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Autowired
	private KafkaBinderConfigurationProperties binderProperties;

	@Test
	void bootKafkaDefaultsAreNotChanged() {
		assertThat(this.environment.getProperty("spring.kafka.consumer.key-deserializer")).isNull();
		assertThat(this.environment.getProperty("spring.kafka.consumer.value-deserializer")).isNull();
		assertThat(this.environment.getProperty("spring.kafka.producer.key-serializer")).isNull();
		assertThat(this.environment.getProperty("spring.kafka.producer.value-serializer")).isNull();
		assertThat(this.kafkaProperties.getConsumer().getKeyDeserializer()).isEqualTo(StringDeserializer.class);
		assertThat(this.kafkaProperties.getConsumer().getValueDeserializer()).isEqualTo(StringDeserializer.class);
		assertThat(this.kafkaProperties.getProducer().getKeySerializer()).isEqualTo(StringSerializer.class);
		assertThat(this.kafkaProperties.getProducer().getValueSerializer()).isEqualTo(StringSerializer.class);
	}

	@Test
	void binderStillUsesByteArrays() {
		Map<String, Object> consumer = BindingUtils.createConsumerConfigs(false, "group",
				new ExtendedConsumerProperties<>(new KafkaConsumerProperties()), this.binderProperties);
		assertThat(consumer.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArrayDeserializer.class);
		assertThat(consumer.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArrayDeserializer.class);
		Map<String, Object> producer = BindingUtils.createProducerConfigs(
				new ExtendedProducerProperties<>(new KafkaProducerProperties()), this.binderProperties);
		assertThat(producer.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArraySerializer.class);
		assertThat(producer.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArraySerializer.class);
	}

	@Test
	void explicitBootSerializersAreStillHonoredByTheBinder() {
		KafkaProperties explicit = new KafkaProperties();
		explicit.getConsumer().setValueDeserializer(LongDeserializer.class);
		explicit.getProducer().setValueSerializer(LongSerializer.class);
		KafkaBinderConfigurationProperties properties = new KafkaBinderConfigurationProperties(explicit,
				new org.springframework.beans.factory.support.DefaultListableBeanFactory().getBeanProvider(
						org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails.class));
		Map<String, Object> consumer = BindingUtils.createConsumerConfigs(false, "group",
				new ExtendedConsumerProperties<>(new KafkaConsumerProperties()), properties);
		assertThat(consumer.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(LongDeserializer.class);
		assertThat(consumer.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArrayDeserializer.class);
		Map<String, Object> producer = BindingUtils.createProducerConfigs(
				new ExtendedProducerProperties<>(new KafkaProducerProperties()), properties);
		assertThat(producer.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(LongSerializer.class);
		assertThat(producer.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArraySerializer.class);
	}

}
