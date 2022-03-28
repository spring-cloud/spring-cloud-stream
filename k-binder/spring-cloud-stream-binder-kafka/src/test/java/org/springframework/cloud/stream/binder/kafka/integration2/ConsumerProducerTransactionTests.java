/*
 * Copyright 2019-2019 the original author or authors.
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

import kafka.server.KafkaConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.kafka.consumer.properties.isolation.level=read_committed",
		"spring.kafka.consumer.enable-auto-commit=false",
		"spring.kafka.consumer.auto-offset-reset=earliest",
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
public class ConsumerProducerTransactionTests {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, "consumer.producer.txOut")
			.brokerProperty(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1")
			.brokerProperty(KafkaConfig.TransactionsTopicMinISRProp(), "1");

	@Autowired
	private Config config;

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				embeddedKafka.getEmbeddedKafka().getBrokersAsString());
		System.setProperty("spring.kafka.bootstrap-servers",
				embeddedKafka.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
		System.clearProperty("spring.kafka.bootstrap-servers");
	}

	@Test
	public void testProducerRunsInConsumerTransaction() throws InterruptedException {
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.outs).containsExactlyInAnyOrder("ONE", "THREE");
	}

	@Test
	public void externalTM() {
		assertThat(this.config.input2Container.getContainerProperties().getTransactionManager())
				.isSameAs(this.config.tm);
		Object handler = KafkaTestUtils.getPropertyValue(this.config.output2, "dispatcher.handlers", Set.class)
				.iterator().next();
		assertThat(KafkaTestUtils.getPropertyValue(handler, "delegate.kafkaTemplate.producerFactory"))
				.isSameAs(this.config.pf);
	}

	@EnableBinding(TwoProcessors.class)
	@EnableAutoConfiguration
	public static class Config {

		final List<String> outs = new ArrayList<>();

		final CountDownLatch latch = new CountDownLatch(2);

		@Autowired
		private MessageChannel output;

		@Autowired
		MessageChannel output2;

		AbstractMessageListenerContainer<?, ?> input2Container;

		ProducerFactory pf;

		KafkaAwareTransactionManager<byte[], byte[]> tm;

		@KafkaListener(id = "test.cons.prod", topics = "consumer.producer.txOut")
		public void listenOut(String in) {
			this.outs.add(in);
			this.latch.countDown();
		}

		@StreamListener(Processor.INPUT)
		public void listenIn(String in) {
			this.output.send(new GenericMessage<>(in.toUpperCase()));
			if (in.equals("two")) {
				throw new RuntimeException("fail");
			}
		}

		@StreamListener("input2")
		public void listenIn2(String in) {
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

	public interface TwoProcessors extends Processor {

		@Input
		SubscribableChannel input2();

		@Output
		MessageChannel output2();

	}

}
