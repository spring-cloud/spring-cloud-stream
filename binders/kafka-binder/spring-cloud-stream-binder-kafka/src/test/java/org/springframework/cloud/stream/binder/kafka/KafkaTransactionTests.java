/*
 * Copyright 2018-present the original author or authors.
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

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.InOrder;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.integration.autoconfigure.IntegrationAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.core.retry.RetryTemplate;

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
 * @author Artem Bilan
 * @since 2.0
 *
 */
@EmbeddedKafka(count = 1, controlledShutdown = true, brokerProperties = {"transaction.state.log.replication.factor=1",
	"transaction.state.log.min.isr=1"})
class KafkaTransactionTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	//@Test
	void producerRunsInTx() throws Exception {
		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
				.singletonList(embeddedKafka.getBrokersAsString()));
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties, mock(ObjectProvider.class));
		configurationProperties.getTransaction().setTransactionIdPrefix("foo-");
		configurationProperties.getTransaction().getProducer().setUseNativeEncoding(true);
		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				configurationProperties, kafkaProperties, prop -> {
		});
		RetryPolicy retryPolicy = RetryPolicy.builder().maxAttempts(2).delay(Duration.ZERO).build();
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate(retryPolicy));
		final Producer mockProducer = mock(Producer.class);
		given(mockProducer.send(any(), any())).willReturn(new CompletableFuture<>());

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
		AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(IntegrationAutoConfiguration.class);
		binder.setApplicationContext(applicationContext);

		// Important: Initialize the binder to trigger onInit()
		binder.afterPropertiesSet();

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
