/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * For more context around this test, see https://github.com/spring-cloud/spring-cloud-stream/issues/2613.
 *
 * @author Soby Chacko
 */
@EmbeddedKafka(topics = "foo-2")
public class EventTypeRoutingWithInferredSerdeTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static Consumer<Integer, Integer> consumer;

	@BeforeAll
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-1", "false",
			embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put("value.deserializer", IntegerDeserializer.class);
		DefaultKafkaConsumerFactory<Integer, Integer> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "foo-2");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void eventTypeRoutingWorksWhenInferredSerdeThrowsException() {
		SpringApplication app = new SpringApplication(EventTypeRoutingWithConfiguredSerdeConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
			"--server.port=0",
			"--spring.jmx.enabled=false",
			"--spring.cloud.function.definition=process",
			"--spring.cloud.stream.bindings.process-in-0.destination=foo-1",
			"--spring.cloud.stream.bindings.process-out-0.destination=foo-2",
			"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.eventTypes=foo,bar",
			"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.useConfiguredSerdeWhenRoutingEvents=true",
			"--spring.cloud.stream.kafka.streams.binder.deserializationExceptionHandler=logAndContinue",
			"--spring.cloud.stream.kafka.streams.binder.functions.process.applicationId=process-id-foo-0",
			"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
			"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put("value.serializer", IntegerSerializer.class);
			DefaultKafkaProducerFactory<Integer, Integer> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, Integer> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("foo-1");

				Headers headers = new RecordHeaders();
				headers.add(new RecordHeader("event_type", "foo".getBytes()));

				final ProducerRecord<Integer, Integer> producerRecord1 = new ProducerRecord<>("foo-1", 0, 56, 56, headers);
				template.send(producerRecord1);


				final ProducerRecord<Integer, Integer> producerRecord2 = new ProducerRecord<>("foo-1", 0, 57, 57);
				template.send(producerRecord2);

				final ProducerRecord<Integer, Integer> producerRecord3 = new ProducerRecord<>("foo-1", 0, 58, 58, headers);
				template.send(producerRecord3);

				Headers headers1 = new RecordHeaders();
				headers1.add(new RecordHeader("event_type", "bar".getBytes()));

				final ProducerRecord<Integer, Integer> producerRecord4 = new ProducerRecord<>("foo-1", 0, 59, 59, headers1);
				template.send(producerRecord4);

				senderProps.put("value.serializer", StringSerializer.class);
				DefaultKafkaProducerFactory<Integer, String> pf1 = new DefaultKafkaProducerFactory<>(senderProps);
				KafkaTemplate<Integer, String> template1 = new KafkaTemplate<>(pf1, true);
				template1.setDefaultTopic("foo-1");
				final ProducerRecord<Integer, String> producerRecordBar = new ProducerRecord<>("foo-1", 0, 67, "foobar", headers);
				// This send should throw a deserializer exception by the Kafka Streams processor.
				// Since the processor is provisioned with an exception handler of logAndContinue, it shouldn't affect anything.
				template1.send(producerRecordBar);

				final ConsumerRecords<Integer, Integer> records = KafkaTestUtils.getRecords(consumer);

				assertThat(records.count()).isEqualTo(3);

				List<Integer> keys = new ArrayList<>();
				List<Integer> values = new ArrayList<>();

				records.forEach(integerFooConsumerRecord -> {
					keys.add(integerFooConsumerRecord.key());
					values.add(integerFooConsumerRecord.value());
				});

				assertThat(keys).containsExactlyInAnyOrder(56, 58, 59);
				assertThat(values).containsExactlyInAnyOrder(56, 58, 59);
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableAutoConfiguration
	public static class EventTypeRoutingWithConfiguredSerdeConfig {

		@Bean
		public Function<KStream<Integer, Integer>, KStream<Integer, Integer>> process() {
			return input -> input;
		}

	}

}
