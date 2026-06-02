/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Nikita Kibitkin
 */
@EmbeddedKafka(topics = {"interceptor-in", "interceptor-out"})
class KafkaStreamsRecordInterceptorTests {

	private static final List<String> INTERCEPTED_RECORDS = new CopyOnWriteArrayList<>();

	private static CountDownLatch interceptorLatch;

	private static EmbeddedKafkaBroker embeddedKafka;

	private static Consumer<Integer, Foo> consumer;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("interceptor-group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put("value.deserializer", JsonDeserializer.class);
		consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		DefaultKafkaConsumerFactory<Integer, Foo> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "interceptor-out");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	void interceptorsAreAppliedBeforeEventTypeRoutingInOrder() throws Exception {
		INTERCEPTED_RECORDS.clear();
		interceptorLatch = new CountDownLatch(4);
		SpringApplication app = new SpringApplication(InterceptorConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=process",
				"--spring.cloud.stream.bindings.process-in-0.destination=interceptor-in",
				"--spring.cloud.stream.bindings.process-out-0.destination=interceptor-out",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.eventTypes=foo",
				"--spring.cloud.stream.kafka.streams.binder.functions.process.applicationId=interceptor-test-id",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			senderProps.put("value.serializer", JsonSerializer.class);
			DefaultKafkaProducerFactory<Integer, Foo> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, Foo> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("interceptor-in");
				Headers headers = new RecordHeaders();
				headers.add(new RecordHeader("event_type", "foo".getBytes()));
				template.send(new ProducerRecord<>("interceptor-in", 0, 1, new Foo("matched"), headers));
				template.send(new ProducerRecord<>("interceptor-in", 0, 2, new Foo("filtered")));

				assertThat(interceptorLatch.await(10, TimeUnit.SECONDS)).isTrue();
				ConsumerRecords<Integer, Foo> records = KafkaTestUtils.getRecords(consumer);

				assertThat(records.count()).isEqualTo(1);
				assertThat(records.iterator().next().key()).isEqualTo(1);
				assertThat(INTERCEPTED_RECORDS).containsExactly(
						"first:process-in-0:interceptor-in",
						"second:process-in-0:interceptor-in",
						"first:process-in-0:interceptor-in",
						"second:process-in-0:interceptor-in");
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableAutoConfiguration
	public static class InterceptorConfiguration {

		@Bean
		public Function<KStream<Integer, Foo>, KStream<Integer, Foo>> process() {
			return input -> input;
		}

		@Bean
		@Order(1)
		public KafkaStreamsRecordInterceptor firstInterceptor() {
			return (record, context) -> {
				INTERCEPTED_RECORDS.add("first:" + context.bindingName() + ":" + context.topic().orElse("unknown"));
				interceptorLatch.countDown();
			};
		}

		@Bean
		@Order(2)
		public KafkaStreamsRecordInterceptor secondInterceptor() {
			return (record, context) -> {
				INTERCEPTED_RECORDS.add("second:" + context.bindingName() + ":" + context.topic().orElse("unknown"));
				interceptorLatch.countDown();
			};
		}

	}

	static class Foo {

		private String foo;

		Foo() {
		}

		Foo(String foo) {
			this.foo = foo;
		}

		public String getFoo() {
			return this.foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Foo foo1 = (Foo) o;
			return Objects.equals(this.foo, foo1.foo);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.foo);
		}

	}

}
