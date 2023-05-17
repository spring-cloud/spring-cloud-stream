/*
 * Copyright 2022-2023 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderResult;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for {@link ReactorKafkaBinder}.
 *
 * @author Soby Chacko
 * @author Gary Russell
 * @author Chris Bono
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(topics = { "uppercased-words", "lowercased-words" })
class ReactorKafkaBinderIntegrationTests {

	private static final List<String> recOptsCustOrder = Collections.synchronizedList(new ArrayList<>());

	private static final List<String> patternedDeliveries = Collections.synchronizedList(new ArrayList<>());

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void endToEndReactorKafkaBinder(boolean excludeKafkaAutoConfig) {

		recOptsCustOrder.clear();
		patternedDeliveries.clear();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group1", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<String, String> consumer1 = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer1, "uppercased-words");
		Consumer<String, String> consumer2 = cf.createConsumer("group2", null);
		embeddedKafka.consumeFromEmbeddedTopics(consumer2, "lowercased-words");

		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(ReactiveKafkaApplication.class)
			.web(WebApplicationType.NONE).run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.kafka.consumer.metadata.max.age.ms=1000",
				"--spring.cloud.function.definition=uppercase;lowercase;patternConsumer",
				"--spring.cloud.stream.function.reactive.uppercase=true",
				"--spring.cloud.stream.function.reactive.lowercase=true",
				"--spring.cloud.stream.bindings.uppercase-in-0.group=grp1",
				"--spring.cloud.stream.bindings.uppercase-in-0.destination=words1",
				"--spring.cloud.stream.bindings.uppercase-out-0.destination=uppercased-words",
				"--spring.cloud.stream.bindings.lowercase-in-0.group=grp2",
				"--spring.cloud.stream.bindings.lowercase-in-0.destination=words2",
				"--spring.cloud.stream.bindings.lowercase-out-0.destination=lowercased-words",
				"--spring.cloud.stream.bindings.patternConsumer-in-0.group=grp3",
				"--spring.cloud.stream.bindings.patternConsumer-in-0.destination=.*-words",
				"--spring.cloud.stream.kafka.bindings.words1.producer.record-metadata-channel=sendResults",
				"--spring.cloud.stream.kafka.bindings.words2.producer.record-metadata-channel=sendResults",
				"--spring.cloud.stream.kafka.bindings.patternConsumer-in-0.consumer.destination-is-pattern=true",
				"--spring.cloud.stream.kafka.bindings.lowercase-in-0.consumer.converterBeanName=fullRR",
				"--spring.cloud.stream.kafka.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				excludeKafkaAutoConfigParam(excludeKafkaAutoConfig))) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			streamBridge.send("words1", MessageBuilder.withPayload("foobar").setCorrelationId(42).build());
			streamBridge.send("words2", MessageBuilder.withPayload("BAZQUX").setCorrelationId(43).build());

			assertThat(KafkaTestUtils.getSingleRecord(consumer1, "uppercased-words"))
				.isNotNull()
				.extracting(ConsumerRecord::value)
				.isEqualTo("FOOBAR");

			assertThat(KafkaTestUtils.getSingleRecord(consumer2, "lowercased-words"))
				.isNotNull()
				.extracting(ConsumerRecord::value)
				.isEqualTo("bazqux");

			assertThat(recOptsCustOrder).containsExactly("two", "one", "two", "one", "two", "one");
			await().untilAsserted(() -> assertThat(patternedDeliveries).contains("bazqux", "FOOBAR"));
			assertThat(context.getBean(ReactiveKafkaApplication.class).correlation).contains(42, 43);
		}
	}

	private String excludeKafkaAutoConfigParam(boolean excludeKafkaAutoConfig) {
		return excludeKafkaAutoConfig ?
			"--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration" : "foo=bar";
	}

	@EnableAutoConfiguration
	public static class ReactiveKafkaApplication {

		final List<Integer> correlation = Collections.synchronizedList(new ArrayList<>());

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
		Function<Flux<String>, Flux<String>> uppercase() {
			return s -> s.map(String::toUpperCase);
		}

		@Bean
		Function<Flux<ReceiverRecord<byte[], byte[]>>, Flux<String>> lowercase() {
			return s -> s.map(rec -> new String(rec.value()).toLowerCase());
		}

		@Bean
		java.util.function.Consumer<Flux<String>> patternConsumer() {
			return f -> f.doOnNext(s -> patternedDeliveries.add(s))
					.subscribe();
		}

		@Bean
		ReceiverOptionsCustomizer<?, ?> cust1() {
			return (t, u) -> {
				recOptsCustOrder.add("one");
				return u;
			};
		}

		@Bean
		ReceiverOptionsCustomizer<String, byte[]> cust2() {
			return new ReceiverOptionsCustomizer<>() {

				@Override
				public ReceiverOptions<String, byte[]> apply(String t, ReceiverOptions<String, byte[]> u) {
					recOptsCustOrder.add("two");
					u.withKeyDeserializer(new StringDeserializer());
					return u;
				}

				@Override
				public int getOrder() {
					return -1;
				}

			};
		}

		@Bean
		FluxMessageChannel sendResults() {
			return new FluxMessageChannel();
		}

		@ServiceActivator(inputChannel = "sendResults")
		void handleResults(SenderResult<Integer> result) {
			this.correlation.add(result.correlationMetadata());
		}

	}

}
