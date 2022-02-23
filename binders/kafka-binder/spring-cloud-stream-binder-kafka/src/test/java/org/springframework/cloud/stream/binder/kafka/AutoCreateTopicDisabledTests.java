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

import kafka.server.KafkaConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import static org.hamcrest.CoreMatchers.isA;

/**
 * @author Soby Chacko
 */
public class AutoCreateTopicDisabledTests {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 1)
			.brokerProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "false");

	@Test
	public void testAutoCreateTopicDisabledFailsOnConsumerIfTopicNonExistentOnBroker()
			throws Throwable {

		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
				.singletonList(embeddedKafka.getEmbeddedKafka().getBrokersAsString()));
		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties);
		// disable auto create topic on the binder.
		configurationProperties.setAutoCreateTopics(false);

		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				configurationProperties, kafkaProperties, null);
		provisioningProvider.setMetadataRetryOperations(new RetryTemplate());

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider);

		final String testTopicName = "nonExistent" + System.currentTimeMillis();

		ExtendedConsumerProperties<KafkaConsumerProperties> properties = new ExtendedConsumerProperties<>(
				new KafkaConsumerProperties());

		expectedException.expect(BinderException.class);
		expectedException.expectCause(isA(UnknownTopicOrPartitionException.class));
		binder.createConsumerEndpoint(() -> testTopicName, "group", properties);
	}

	@Test
	public void testAutoCreateTopicDisabledFailsOnProducerIfTopicNonExistentOnBroker()
			throws Throwable {

		KafkaProperties kafkaProperties = new TestKafkaProperties();
		kafkaProperties.setBootstrapServers(Collections
				.singletonList(embeddedKafka.getEmbeddedKafka().getBrokersAsString()));

		KafkaBinderConfigurationProperties configurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties);
		// disable auto create topic on the binder.
		configurationProperties.setAutoCreateTopics(false);
		// reduce the wait time on the producer blocking operations.
		configurationProperties.getConfiguration().put("max.block.ms", "3000");

		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(
				configurationProperties, kafkaProperties, null);
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(1);
		final RetryTemplate metadataRetryOperations = new RetryTemplate();
		metadataRetryOperations.setRetryPolicy(simpleRetryPolicy);
		provisioningProvider.setMetadataRetryOperations(metadataRetryOperations);

		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider);

		final String testTopicName = "nonExistent" + System.currentTimeMillis();

		ExtendedProducerProperties<KafkaProducerProperties> properties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());

		expectedException.expect(BinderException.class);
		expectedException.expectCause(isA(UnknownTopicOrPartitionException.class));

		binder.bindProducer(testTopicName, new DirectChannel(), properties);

	}

}
