/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.example.Sensor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.function.context.converter.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.function.context.converter.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.binder.kafka.streams.integration.utils.TestAvroSerializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * @author Soby Chacko
 */
public class PerRecordAvroContentTypeTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"received-sensors");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	private static Consumer<String, byte[]> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("avro-ct-test",
				"false", embeddedKafka);

		// Receive the data as byte[]
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);

		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, byte[]> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "received-sensors");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testPerRecordAvroConentTypeAndVerifySerialization() throws Exception {
		SpringApplication app = new SpringApplication(SensorCountAvroApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext ignored = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.consumer.useNativeDecoding=false",
				"--spring.cloud.stream.bindings.output.producer.useNativeEncoding=false",
				"--spring.cloud.stream.bindings.input.destination=sensors",
				"--spring.cloud.stream.bindings.output.destination=received-sensors",
				"--spring.cloud.stream.bindings.output.contentType=application/avro",
				"--spring.cloud.stream.kafka.streams.bindings.input.consumer.application-id=per-record-avro-contentType-test",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.brokers="
						+ embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			// Use a custom avro test serializer
			senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					TestAvroSerializer.class);
			DefaultKafkaProducerFactory<Integer, Sensor> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			try {
				KafkaTemplate<Integer, Sensor> template = new KafkaTemplate<>(pf, true);

				Random random = new Random();
				Sensor sensor = new Sensor();
				sensor.setId(UUID.randomUUID().toString() + "-v1");
				sensor.setAcceleration(random.nextFloat() * 10);
				sensor.setVelocity(random.nextFloat() * 100);
				sensor.setTemperature(random.nextFloat() * 50);
				// Send with avro content type set.
				Message<?> message = MessageBuilder.withPayload(sensor)
						.setHeader("contentType", "application/avro").build();
				template.setDefaultTopic("sensors");
				template.send(message);

				// Serialized byte[] ^^ is received by the binding process and deserialzed
				// it using avro converter.
				// Then finally, the data will be output to a return topic as byte[]
				// (using the same avro converter).

				// Receive the byte[] from return topic
				ConsumerRecord<String, byte[]> cr = KafkaTestUtils
						.getSingleRecord(consumer, "received-sensors");
				final byte[] value = cr.value();

				// Convert the byte[] received back to avro object and verify that it is
				// the same as the one we sent ^^.
				AvroSchemaMessageConverter avroSchemaMessageConverter = new AvroSchemaMessageConverter(new AvroSchemaServiceManagerImpl());

				Message<?> receivedMessage = MessageBuilder.withPayload(value)
						.setHeader("contentType",
								MimeTypeUtils.parseMimeType("application/avro"))
						.build();
				Sensor messageConverted = (Sensor) avroSchemaMessageConverter
						.fromMessage(receivedMessage, Sensor.class);
				assertThat(messageConverted).isEqualTo(sensor);
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	static class SensorCountAvroApplication {

		@StreamListener
		@SendTo("output")
		public KStream<?, Sensor> process(@Input("input") KStream<Object, Sensor> input) {
			// return the same Sensor object unchanged so that we can do test
			// verifications
			return input.map(KeyValue::new);
		}

		@Bean
		public MessageConverter sensorMessageConverter() throws IOException {
			return new AvroSchemaMessageConverter(new AvroSchemaServiceManagerImpl());
		}

	}

}
