/*
 * Copyright 2018 the original author or authors.
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

package com.example.kafka.streams.branching;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
		topics = { "words", "english-counts", "french-counts", "spanish-counts" },
		bootstrapServersProperty = "spring.cloud.stream.kafka.streams.binder.brokers",
		controlledShutdown = true)
class KafkaStreamsBranchingSampleTests {

	@BeforeEach
	void setKafkaStreamsCloseTimeout(@Autowired StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		streamsBuilderFactoryBean.setCloseTimeout(0);
	}

	@Test
	void wordEventsDirectedToProperTopic(@Autowired EmbeddedKafkaBroker embeddedKafka) throws InterruptedException {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("words");
			template.sendDefault("english");
			template.sendDefault("french");
			template.sendDefault("spanish");
		}
		finally {
			pf.destroy();
		}

		TimeUnit.SECONDS.sleep(2);

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("wordEventsTest", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		try (Consumer<String, String> consumer = cf.createConsumer()) {
			embeddedKafka.consumeFromEmbeddedTopics(consumer, "english-counts", "french-counts", "spanish-counts");
			assertThat(KafkaTestUtils.getSingleRecord(consumer, "english-counts", 5000L))
					.extracting(ConsumerRecord::value)
					.asInstanceOf(InstanceOfAssertFactories.STRING)
					.contains("english");
			assertThat(KafkaTestUtils.getSingleRecord(consumer, "french-counts", 5000L))
					.extracting(ConsumerRecord::value)
					.asInstanceOf(InstanceOfAssertFactories.STRING)
					.contains("french");
			assertThat(KafkaTestUtils.getSingleRecord(consumer, "spanish-counts", 5000L))
					.extracting(ConsumerRecord::value)
					.asInstanceOf(InstanceOfAssertFactories.STRING)
					.contains("spanish");
		}
	}
}
