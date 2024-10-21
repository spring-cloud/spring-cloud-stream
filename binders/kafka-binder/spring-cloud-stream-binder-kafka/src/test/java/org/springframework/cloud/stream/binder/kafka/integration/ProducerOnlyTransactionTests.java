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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.Locale;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Soby Chacko
 * @since 2.1.4
 *
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.stream.kafka.binder.transaction.transaction-id-prefix=tx.",
		"spring.cloud.stream.kafka.binder.transaction.producer.configuration.retries=99",
		"spring.cloud.stream.kafka.binder.transaction.producer.configuration.acks=all"})
@DirtiesContext
@EmbeddedKafka(topics = "output", controlledShutdown = true, brokerProperties = {"transaction.state.log.replication.factor=1",
	"transaction.state.log.min.isr=1"})
class ProducerOnlyTransactionTests {

	@Autowired
	private Sender sender;

	@Autowired
	private ApplicationContext context;

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBrokera;

	@Test
	void producerTx() {
		final StreamBridge streamBridge = context.getBean(StreamBridge.class);
		this.sender.DoInTransaction(streamBridge);
		assertThat(this.sender.isInTx()).isTrue();
		Map<String, Object> props = KafkaTestUtils.consumerProps("consumeTx", "false",
			embeddedKafkaBrokera);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
		Consumer<?, ?> consumer = new KafkaConsumer<>(props);
		embeddedKafkaBrokera.consumeFromAllEmbeddedTopics(consumer);
		ConsumerRecord<?, ?> record = KafkaTestUtils.getSingleRecord(consumer, "output");
		assertThat(record.value()).isEqualTo("foo".getBytes());
	}

	@EnableAutoConfiguration
	@EnableTransactionManagement
	@Configuration
	public static class Config {

		@Bean
		public PlatformTransactionManager transactionManager(BinderFactory binders) {
			try {
				ProducerFactory<byte[], byte[]> pf = ((KafkaMessageChannelBinder) binders.getBinder(null,
						MessageChannel.class)).getTransactionalProducerFactory();
				KafkaTransactionManager<byte[], byte[]> tm = new KafkaTransactionManager<>(pf);
				tm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
				return tm;
			}
			catch (BeanCreationException e) { // needed to avoid other tests in this package failing when there is no binder
				return null;
			}
		}

		@Bean
		public Sender sender() {
			return new Sender();
		}

	}

	public static class Sender {

		private boolean isInTx;

		@Transactional
		public void DoInTransaction(StreamBridge streamBridge) {
			this.isInTx = TransactionSynchronizationManager.isActualTransactionActive();
			streamBridge.send("output", new GenericMessage<>("foo".getBytes()));
		}

		public boolean isInTx() {
			return this.isInTx;
		}

	}

}
