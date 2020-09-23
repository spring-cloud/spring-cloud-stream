/*
 * Copyright 2020-2020 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.config.ConsumerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.config.ProducerConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 *
 * Based on: https://github.com/spring-projects/spring-kafka/issues/897#issuecomment-466060097
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {"spring.cloud.function.definition=process",
		"spring.cloud.stream.bindings.process-in-0.group=KafkaConfigCustomizationTests.group"})
@DirtiesContext
public class KafkaConfigCustomizationTests {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	@ClassRule
	public static EmbeddedKafkaRule kafkaEmbedded = new EmbeddedKafkaRule(1, true);

	static final CountDownLatch countDownLatch = new CountDownLatch(2);

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				kafkaEmbedded.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@Test
	public void testBothConsumerAndProducerConfigsCanBeCustomized() throws InterruptedException {
		Map<String, Object> producerProps = KafkaTestUtils
				.producerProps(kafkaEmbedded.getEmbeddedKafka());
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(
				new DefaultKafkaProducerFactory<>(producerProps));
		template.send("process-in-0", "test-foo");
		template.flush();
		assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@SpringBootApplication
	public static class ConfigCustomizerTestConfig {

		@Bean
		public Function<String, String> process() {
			return payload -> payload;
		}

		@Bean
		public ConsumerConfigCustomizer consumerConfigCustomizer() {
			return consumerProperties -> {
				consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());
				consumerProperties.put("foo.bean", foo());
			};
		}

		@Bean
		public ProducerConfigCustomizer producerConfigCustomizer() {
			return producerProperties -> {
				producerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
				producerProperties.put("foo.bean", foo());
			};
		}

		@Bean
		public Foo foo() {
			return new Foo();
		}
	}

	public static class Foo {

		public void foo(String what) {
			KafkaConfigCustomizationTests.countDownLatch.countDown();
		}

	}

	public static class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {

		private Foo foo;

		@Override
		public void configure(Map<String, ?> configs) {
			this.foo = (Foo) configs.get("foo.bean");
		}

		@Override
		public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
			this.foo.foo("consumer interceptor");
			return records;
		}

		@Override
		public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

		}

		@Override
		public void close() {
		}

	}

	public static class MyProducerInterceptor implements ProducerInterceptor<String, String> {

		private Foo foo;

		@Override
		public void configure(Map<String, ?> configs) {
			this.foo = (Foo) configs.get("foo.bean");
		}

		@Override
		public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
			this.foo.foo("producer interceptor");
			return record;
		}

		@Override
		public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		}

		@Override
		public void close() {
		}
	}
}
