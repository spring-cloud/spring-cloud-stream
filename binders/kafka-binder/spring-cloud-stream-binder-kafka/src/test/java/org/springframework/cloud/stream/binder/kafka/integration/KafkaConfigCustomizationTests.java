/*
 * Copyright 2020-2024 the original author or authors.
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
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.BinderCustomizer;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.config.ClientFactoryCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ConsumerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ProducerConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 * @author Artem Bilan
 *
 * Based on: https://github.com/spring-projects/spring-kafka/issues/897#issuecomment-466060097
 */
@SpringBootTest(properties = {"spring.cloud.function.definition=process",
		"spring.cloud.stream.bindings.process-in-0.group=KafkaConfigCustomizationTests.group"})
@DirtiesContext
@EmbeddedKafka
class KafkaConfigCustomizationTests {

	static final CountDownLatch countDownLatch = new CountDownLatch(2);

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	ConfigCustomizerTestConfig configCustomizerTestConfig;

	@Test
	void bothConsumerAndProducerConfigsCanBeCustomized() throws InterruptedException {
		Map<String, Object> producerProps = KafkaTestUtils
				.producerProps(embeddedKafkaBroker);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(
				new DefaultKafkaProducerFactory<>(producerProps));
		template.send("process-in-0", "test-foo");
		template.flush();
		assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(this.configCustomizerTestConfig.producerFactoryCustomized).isTrue();
		assertThat(this.configCustomizerTestConfig.consumerFactoryCustomized).isTrue();
	}

	@SpringBootApplication
	public static class ConfigCustomizerTestConfig {

		@Bean
		public Function<String, String> process() {
			return payload -> payload;
		}

		@Bean
		public ConsumerConfigCustomizer consumerConfigCustomizer() {
			return (consumerProperties, binding, destination) -> {
				assertThat(binding).isEqualTo("process-in-0");
				assertThat(destination).isEqualTo("process-in-0");
				consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyConsumerInterceptor.class.getName());
				consumerProperties.put("foo.bean", foo());
			};
		}

		@Bean
		public ProducerConfigCustomizer producerConfigCustomizer() {
			return (producerProperties, binding, destination) -> {
				assertThat(binding).isEqualTo("process-out-0");
				assertThat(destination).isEqualTo("process-out-0");
				producerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
				producerProperties.put("foo.bean", foo());
			};
		}

		@Bean
		public Foo foo() {
			return new Foo();
		}

		private boolean producerFactoryCustomized;

		private boolean consumerFactoryCustomized;

		@Bean
		BinderCustomizer binderCustomizer() {
			return (binder, binderName) -> {
				if (binder instanceof KafkaMessageChannelBinder kafkaMessageChannelBinder) {
					kafkaMessageChannelBinder.addClientFactoryCustomizer(new ClientFactoryCustomizer() {
						@Override
						public void configure(ConsumerFactory<?, ?> cf) {
							consumerFactoryCustomized = true;
						}

						@Override
						public void configure(ProducerFactory<?, ?> pf) {
							producerFactoryCustomized = true;
						}
					});
				}
			};
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
