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

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.core.retry.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

/**
 * @author Soby Chacko
 * @author Artem Bilan
 */
@EmbeddedKafka(brokerProperties = {"auto.create.topics.enable=false"})
class AutoCreateTopicDisabledTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	@SuppressWarnings("unchecked")
	void autoCreateTopicDisabledFailsOnConsumerIfTopicNonExistentOnBroker() {

		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
			.singletonList(embeddedKafka.getBrokersAsString()));
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
			kafkaProperties, mock(ObjectProvider.class));
		// disable auto create topic on the binder.
		configurationProperties.setAutoCreateTopics(false);

		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
			configurationProperties, kafkaProperties, prop -> {
		});
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
			configurationProperties, provisioningProvider);

		final String testTopicName = "nonExistent" + System.currentTimeMillis();

		ExtendedConsumerProperties<KafkaConsumerProperties> properties = new ExtendedConsumerProperties<>(
			new KafkaConsumerProperties());

		assertThatExceptionOfType(BinderException.class)
			.isThrownBy(() -> binder.createConsumerEndpoint(() -> testTopicName, "group", properties))
			.withRootCauseExactlyInstanceOf(UnknownTopicOrPartitionException.class);
	}

	@Test
	@SuppressWarnings("unchecked")
	void autoCreateTopicDisabledFailsOnProducerIfTopicNonExistentOnBroker() {

		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
				.singletonList(embeddedKafka.getBrokersAsString()));

		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties, mock(ObjectProvider.class));
		// disable auto create topic on the binder.
		configurationProperties.setAutoCreateTopics(false);
		// reduce the wait time on the producer blocking operations.
		configurationProperties.getConfiguration().put("max.block.ms", "3000");

		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				configurationProperties, kafkaProperties, prop -> {
		});
		RetryPolicy retryPolicy = RetryPolicy.builder().maxRetries(1).delay(Duration.ZERO).build();
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate(retryPolicy));

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider);

		final String testTopicName = "nonExistent" + System.currentTimeMillis();

		ExtendedProducerProperties<KafkaProducerProperties> properties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());

		assertThatExceptionOfType(BinderException.class)
			.isThrownBy(() -> binder.bindProducer(testTopicName, new DirectChannel(), properties))
			.withRootCauseExactlyInstanceOf(UnknownTopicOrPartitionException.class);
	}
}
