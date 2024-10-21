/*
 * Copyright 2022-2024 the original author or authors.
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
import java.time.Duration;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import brave.handler.SpanHandler;
import brave.test.TestSpanHandler;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.brave.bridge.BraveFinishedSpan;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.observation.KafkaReceiverObservation;
import reactor.kafka.receiver.observation.KafkaRecordReceiverContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Artem Bilan
 * @author Soby Chacko
 * @since 4.2.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
	"spring.kafka.consumer.metadata.max.age.ms=1000",
	"spring.cloud.function.definition=receive",
	"spring.cloud.stream.function.reactive.uppercase=true",
	"spring.cloud.stream.bindings.receive-in-0.group=rkbot-in-group",
	"spring.cloud.stream.bindings.receive-in-0.destination=rkbot-in-topic",
	"spring.cloud.stream.bindings.receive-out-0.destination=rkbot-out-topic",
	"spring.cloud.stream.kafka.binder.enable-observation=true",
	"spring.cloud.stream.kafka.binder.brokers=${spring.kafka.bootstrap-servers}",
	"management.tracing.sampling.probability=1",
	"spring.cloud.stream.kafka.bindings.receive-in-0.consumer.converterBeanName=fullRR"
	})
@DirtiesContext
@AutoConfigureObservability
@EmbeddedKafka(topics = { "rkbot-out-topic" })
public class ReactorKafkaBinderObservationTests {

	private static final TestSpanHandler SPANS = new TestSpanHandler();

	@Autowired
	StreamBridge streamBridge;

	@Autowired
	ObservationRegistry observationRegistry;

	@Autowired
	TestConfiguration testConfiguration;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Test
	void endToEndReactorKafkaBinder1() {

		streamBridge.send("rkbot-in-topic", MessageBuilder.withPayload("data")
			.build());

		await().timeout(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(SPANS.spans()).hasSize(3));
		SpansAssert.assertThat(SPANS.spans().stream().map(BraveFinishedSpan::fromBrave).collect(Collectors.toList()))
			.haveSameTraceId();
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration(exclude = org.springframework.cloud.function.observability.ObservationAutoConfiguration.class)
	public static class TestConfiguration {

		@Bean
		SpanHandler testSpanHandler() {
			return SPANS;
		}

		@Bean
		RecordMessageConverter fullRR() {
			return new RecordMessageConverter() {

				private final RecordMessageConverter converter = new MessagingMessageConverter();

				@Override
				public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
											org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, Type payloadType) {

					return MessageBuilder.withPayload(record).build();
				}

				@Override
				public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
					return this.converter.fromMessage(message, defaultTopic);
				}

			};
		}

		@Bean
		Function<Flux<ReceiverRecord<byte[], byte[]>>, Flux<Message<String>>> receive(ObservationRegistry observationRegistry) {
			return s -> s
				.flatMap(record -> {
					Observation receiverObservation =
						KafkaReceiverObservation.RECEIVER_OBSERVATION.start(null,
							KafkaReceiverObservation.DefaultKafkaReceiverObservationConvention.INSTANCE,
							() ->
								new KafkaRecordReceiverContext(
									record, "user.receiver", "localhost:9092"),
							observationRegistry);

					return Mono.deferContextual(contextView -> Mono.just(record)
							.map(rec -> new String(rec.value()).toLowerCase(Locale.ROOT))
							.map(rec -> MessageBuilder.withPayload(rec).setHeader(IntegrationMessageHeaderAccessor.REACTOR_CONTEXT, contextView).build()))
						.doOnTerminate(receiverObservation::stop)
						.doOnError(receiverObservation::error)
						.contextWrite(context -> context.put(ObservationThreadLocalAccessor.KEY, receiverObservation));
				});
		}
	}

}

