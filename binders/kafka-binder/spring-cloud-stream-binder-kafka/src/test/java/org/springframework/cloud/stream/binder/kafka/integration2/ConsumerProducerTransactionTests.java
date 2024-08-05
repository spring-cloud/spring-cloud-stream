/*
 * Copyright 2019-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Soby Chacko
 * @since 3.0
 *
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.kafka.consumer.properties.isolation.level=read_committed",
		"spring.kafka.consumer.enable-auto-commit=false",
		"spring.kafka.consumer.auto-offset-reset=earliest",
		"spring.cloud.function.definition=listenIn;listenIn2",
		"spring.cloud.stream.function.bindings.listenIn-in-0=input",
		"spring.cloud.stream.function.bindings.listenIn-out-0=output",
		"spring.cloud.stream.function.bindings.listenIn2-in-0=input2",
		"spring.cloud.stream.function.bindings.listenIn2-out-0=output2",
		"spring.cloud.stream.bindings.input.destination=consumer.producer.txIn",
		"spring.cloud.stream.bindings.input.group=consumer.producer.tx",
		"spring.cloud.stream.bindings.input.consumer.max-attempts=1",
		"spring.cloud.stream.kafka.bindings.input2.consumer.transaction-manager=tm",
		"spring.cloud.stream.kafka.bindings.output2.producer.transaction-manager=tm",
		"spring.cloud.stream.bindings.output.destination=consumer.producer.txOut",
		"spring.cloud.stream.kafka.binder.transaction.transaction-id-prefix=tx.",
		"spring.cloud.stream.kafka.binder.transaction.producer.configuration.retries=99",
		"spring.cloud.stream.kafka.binder.transaction.producer.configuration.acks=all"})
@DirtiesContext
@EmbeddedKafka(topics = "consumer.producer.txOut", controlledShutdown = true, brokerProperties = {"transaction.state.log.replication.factor=1",
	"transaction.state.log.min.isr=1"})
@Disabled
class ConsumerProducerTransactionTests {

	@Autowired
	private Config config;

	@Autowired
	private ApplicationContext context;

	@Test
	public void producerRunsInConsumerTransaction() throws InterruptedException {
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.outs).containsExactlyInAnyOrder("ONE", "THREE");
	}

	@Test
	void externalTM() {
		assertThat(this.config.input2Container.getContainerProperties().getTransactionManager())
				.isSameAs(this.config.tm);
		final MessageChannel output2 = context.getBean("output2", MessageChannel.class);

		Object handler = KafkaTestUtils.getPropertyValue(output2, "dispatcher.handlers", Set.class)
				.iterator().next();
		assertThat(KafkaTestUtils.getPropertyValue(handler, "delegate.kafkaTemplate.producerFactory"))
				.isSameAs(this.config.pf);
	}

	@EnableAutoConfiguration
	@Configuration
	public static class Config {

		final List<String> outs = new ArrayList<>();

		final CountDownLatch latch = new CountDownLatch(2);

		AbstractMessageListenerContainer<?, ?> input2Container;

		ProducerFactory pf;

		KafkaAwareTransactionManager<byte[], byte[]> tm;

		@KafkaListener(id = "test.cons.prod", topics = "consumer.producer.txOut")
		public void listenOut(String in) {
			this.outs.add(in);
			this.latch.countDown();
		}

		@Bean
		public Function<String, String> listenIn() {
			return in -> {
				if (in.equals("two")) {
					throw new RuntimeException("fail");
				}
				return in.toUpperCase();
			};
		}

		@Bean
		public Function<String, String> listenIn2() {
			return in -> in;
		}

		@Bean
		public ApplicationRunner runner(KafkaTemplate<byte[], byte[]> template) {
			return args -> {
				template.send("consumer.producer.txIn", "one".getBytes());
				template.send("consumer.producer.txIn", "two".getBytes());
				template.send("consumer.producer.txIn", "three".getBytes());
			};
		}

		@Bean
		public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer() {
			return (container, dest, group) -> {
				container.setAfterRollbackProcessor(new DefaultAfterRollbackProcessor<>(new FixedBackOff(0L, 1L)));
				if ("input2".equals(dest)) {
					this.input2Container = container;
				}
			};
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public KafkaAwareTransactionManager<byte[], byte[]> tm(ProducerFactory pf) {
			KafkaAwareTransactionManager mock = mock(KafkaAwareTransactionManager.class);
			this.pf = pf;
			given(mock.getProducerFactory()).willReturn(pf);
			this.tm = mock;
			return mock;
		}
	}
}

