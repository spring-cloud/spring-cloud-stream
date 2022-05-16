/*
 * Copyright 2020-2022 the original author or authors.
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

package com.example.kafkanativeserialization;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static com.example.kafkanativeserialization.KafkaNativeSerializationApplicationTests.INPUT_TOPIC;
import static com.example.kafkanativeserialization.KafkaNativeSerializationApplicationTests.OUTPUT_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
	topics = { INPUT_TOPIC, OUTPUT_TOPIC },
	bootstrapServersProperty = "spring.cloud.stream.kafka.binder.brokers",
	controlledShutdown = true)
class KafkaNativeSerializationApplicationTests {

	public static final String INPUT_TOPIC = "topic1";
	public static final String OUTPUT_TOPIC = "topic2";

	@Test
	void functionOutputPersonWithNativeEncoding(@Autowired EmbeddedKafkaBroker embeddedKafka) {

		// Send 'foo' to input topic
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		DefaultKafkaProducerFactory<byte[], String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<byte[], String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic(INPUT_TOPIC);
		template.sendDefault("foo");

		// Consume from output topic w/ custom JSON deserializer
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("nativeSerializationTest", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyJsonDeserializer.class);
		DefaultKafkaConsumerFactory<byte[], Person> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		try (Consumer<byte[], Person> consumer = cf.createConsumer()) {
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer, OUTPUT_TOPIC);
			assertThat(KafkaTestUtils.getSingleRecord(consumer, OUTPUT_TOPIC, 10000L))
				.extracting(ConsumerRecord::value)
				.extracting(Person::getName)
				.isEqualTo("foo");
		}
	}
}
