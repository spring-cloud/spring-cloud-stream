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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.Map;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.1.4
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.stream.kafka.binder.transaction.transaction-id-prefix=tx.",
		"spring.cloud.stream.kafka.binder.transaction.producer.configuration.retries=99",
		"spring.cloud.stream.kafka.binder.transaction.producer.configuration.acks=all"})
@DirtiesContext
public class ProducerOnlyTransactionTests {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, "output")
			.brokerProperty(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1")
			.brokerProperty(KafkaConfig.TransactionsTopicMinISRProp(), "1");

	@Autowired
	private Sender sender;

	@Autowired
	private MessageChannel output;

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				embeddedKafka.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@Test
	public void testProducerTx() {
		this.sender.DoInTransaction(this.output);
		assertThat(this.sender.isInTx()).isTrue();
		Map<String, Object> props = KafkaTestUtils.consumerProps("consumeTx", "false",
				embeddedKafka.getEmbeddedKafka());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
		Consumer<?, ?> consumer = new KafkaConsumer<>(props);
		embeddedKafka.getEmbeddedKafka().consumeFromAllEmbeddedTopics(consumer);
		ConsumerRecord<?, ?> record = KafkaTestUtils.getSingleRecord(consumer, "output");
		assertThat(record.value()).isEqualTo("foo".getBytes());
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@EnableTransactionManagement
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
		public void DoInTransaction(MessageChannel output) {
			this.isInTx = TransactionSynchronizationManager.isActualTransactionActive();
			output.send(new GenericMessage<>("foo"));
		}

		public boolean isInTx() {
			return this.isInTx;
		}

	}

}
