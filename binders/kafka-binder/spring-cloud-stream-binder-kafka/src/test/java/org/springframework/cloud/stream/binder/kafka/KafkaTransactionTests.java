/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.util.Collections;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.InOrder;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class KafkaTransactionTests {

	@ClassRule
	public static final EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1)
			.brokerProperty("transaction.state.log.replication.factor", "1")
			.brokerProperty("transaction.state.log.min.isr", "1");

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testProducerRunsInTx() {
		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
				.singletonList(embeddedKafka.getEmbeddedKafka().getBrokersAsString()));
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties);
		configurationProperties.getTransaction().setTransactionIdPrefix("foo-");
		configurationProperties.getTransaction().getProducer().setUseNativeEncoding(true);
		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				configurationProperties, kafkaProperties, prop -> {
		});
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate());
		final Producer mockProducer = mock(Producer.class);
		given(mockProducer.send(any(), any())).willReturn(new SettableListenableFuture<>());

		KafkaProducerProperties extension1 = configurationProperties
				.getTransaction().getProducer().getExtension();
		extension1.getConfiguration().put(ProducerConfig.RETRIES_CONFIG, "1");
		extension1.getConfiguration().put(ProducerConfig.ACKS_CONFIG, "all");

		willReturn(Collections.singletonList(new TopicPartition("foo", 0)))
				.given(mockProducer).partitionsFor(anyString());
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider) {

			@Override
			protected DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(
					String transactionIdPrefix,
					ExtendedProducerProperties<KafkaProducerProperties> producerProperties, String beanName, String destination) {
				DefaultKafkaProducerFactory<byte[], byte[]> producerFactory = spy(
						super.getProducerFactory(transactionIdPrefix,
								producerProperties, beanName, destination));
				willReturn(mockProducer).given(producerFactory).createProducer("foo-");
				return producerFactory;
			}

		};
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.refresh();
		binder.setApplicationContext(applicationContext);
		DirectChannel channel = new DirectChannel();
		KafkaProducerProperties extension = new KafkaProducerProperties();
		ExtendedProducerProperties<KafkaProducerProperties> properties = new ExtendedProducerProperties<>(
				extension);
		binder.bindProducer("foo", channel, properties);
		channel.send(new GenericMessage<>("foo".getBytes()));
		InOrder inOrder = inOrder(mockProducer);
		inOrder.verify(mockProducer).beginTransaction();
		inOrder.verify(mockProducer).send(any(ProducerRecord.class), any(Callback.class));
		inOrder.verify(mockProducer).commitTransaction();
		inOrder.verify(mockProducer).close(any());
		inOrder.verifyNoMoreInteractions();
		assertThat(TestUtils.getPropertyValue(channel,
				"dispatcher.theOneHandler.useNativeEncoding", Boolean.class)).isTrue();
	}

}
