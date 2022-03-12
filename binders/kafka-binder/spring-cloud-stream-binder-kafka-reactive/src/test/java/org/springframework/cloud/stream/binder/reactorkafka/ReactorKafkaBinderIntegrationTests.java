/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 * @author Gary Russell
 */
@EmbeddedKafka(topics = { "uppercased-words", "lowercased-words" })
public class ReactorKafkaBinderIntegrationTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static Consumer<String, String> consumer1;

	private static Consumer<String, String> consumer2;

	@BeforeAll
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer1 = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer1, "uppercased-words");
		consumer2 = cf.createConsumer("group2", null);
		embeddedKafka.consumeFromEmbeddedTopics(consumer2, "lowercased-words");
	}

	@Test
	void testEndtoEndReactorKafkaBinder() throws Exception {
		SpringApplication app = new SpringApplication(ReactiveKafkaApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase;lowercase",
				"--spring.cloud.stream.function.reactive.uppercase=true",
				"--spring.cloud.stream.function.reactive.lowercase=true",
				"--spring.cloud.stream.bindings.uppercase-in-0.group=grp1",
				"--spring.cloud.stream.bindings.uppercase-in-0.destination=words",
				"--spring.cloud.stream.bindings.uppercase-out-0.destination=uppercased-words",
				"--spring.cloud.stream.bindings.lowercase-in-0.group=grp2",
				"--spring.cloud.stream.bindings.lowercase-in-0.destination=words1",
				"--spring.cloud.stream.bindings.lowercase-out-0.destination=lowercased-words",
				"--spring.cloud.stream.kafka.bindings.lowercase-in-0.consumer.converterBeanName=fullRR",
				"--spring.cloud.stream.kafka.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.send("words", "foobar");
				template.send("words1", "BAZQUX");

				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1, "uppercased-words");
				assertThat(cr.value()).isEqualTo("FOOBAR");
				cr = KafkaTestUtils.getSingleRecord(consumer2, "lowercased-words");
				assertThat(cr.value()).isEqualTo("bazqux");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableAutoConfiguration
	public static class ReactiveKafkaApplication {

		@Bean
		RecordMessageConverter fullRR() {
			return new RecordMessageConverter() {

				private final RecordMessageConverter converter = new MessagingMessageConverter();

				@Override
				public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
						Consumer<?, ?> consumer, Type payloadType) {

					return MessageBuilder.withPayload(record).build();
				}

				@Override
				public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
					return this.converter.fromMessage(message, defaultTopic);
				}

			};
		}

		@Bean
		public Function<Flux<String>, Flux<String>> uppercase() {
			return s -> s.map(String::toUpperCase);
		}

		@Bean
		public Function<Flux<ReceiverRecord<byte[], byte[]>>, Flux<String>> lowercase() {
			return s -> s.map(rec -> new String(rec.value()).toLowerCase());
		}

	}

}
