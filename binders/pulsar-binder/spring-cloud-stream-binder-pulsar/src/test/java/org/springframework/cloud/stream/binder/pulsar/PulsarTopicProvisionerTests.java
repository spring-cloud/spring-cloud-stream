/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarProducerProperties;
import org.springframework.cloud.stream.binder.pulsar.provisioning.PulsarTopicProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
class PulsarTopicProvisionerTests {

	@Test
	void provisionThroughProducerBindingWithDefaultPartitioning() {
		PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties = new PulsarBinderConfigurationProperties();
		PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				pulsarBinderConfigurationProperties);
		ExtendedProducerProperties<PulsarProducerProperties> properties = new ExtendedProducerProperties<>(
				new PulsarProducerProperties());
		ProducerDestination producerDestination = pulsarTopicProvisioner.provisionProducerDestination("foo",
				properties);
		verifyAndAssert(pulsarAdministration, producerDestination.getName(), "foo", 0);
	}

	private static void verifyAndAssert(PulsarAdministration pulsarAdministration, String actualProducerDestination,
			String expectedProducerDestination, int expectedPartitionCount) {
		ArgumentCaptor<PulsarTopic> pulsarTopicArgumentCaptor = ArgumentCaptor.forClass(PulsarTopic.class);
		verify(pulsarAdministration, times(1)).createOrModifyTopics(pulsarTopicArgumentCaptor.capture());
		assertThat(actualProducerDestination).isEqualTo(expectedProducerDestination);
		PulsarTopic pulsarTopic = pulsarTopicArgumentCaptor.getValue();
		assertThat(pulsarTopic.topicName()).isEqualTo(expectedProducerDestination);
		assertThat(pulsarTopic.numberOfPartitions()).isEqualTo(expectedPartitionCount);
	}

	@Test
	void provisionThroughConsumerBindingWithDefaultPartitioning() {
		PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties = new PulsarBinderConfigurationProperties();
		PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				pulsarBinderConfigurationProperties);
		ExtendedConsumerProperties<PulsarConsumerProperties> properties = new ExtendedConsumerProperties<>(
				new PulsarConsumerProperties());
		ConsumerDestination consumerDestination = pulsarTopicProvisioner.provisionConsumerDestination("bar", "",
				properties);
		verifyAndAssert(pulsarAdministration, consumerDestination.getName(), "bar", 0);
	}

	@Test
	void provisioningOnProducerBindingWithPartitionsSetAtTheBinderProperties() {
		PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties = new PulsarBinderConfigurationProperties();
		pulsarBinderConfigurationProperties.setPartitionCount(4);
		PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				pulsarBinderConfigurationProperties);
		ExtendedProducerProperties<PulsarProducerProperties> properties = new ExtendedProducerProperties<>(
				new PulsarProducerProperties());
		ProducerDestination producerDestination = pulsarTopicProvisioner.provisionProducerDestination("foo",
				properties);
		verifyAndAssert(pulsarAdministration, producerDestination.getName(), "foo", 4);
	}

	@Test
	void provisioningOnProducerBindingWithPartitionsSetAtTheBindingProperties() {
		PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties = new PulsarBinderConfigurationProperties();
		PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				pulsarBinderConfigurationProperties);
		ExtendedProducerProperties<PulsarProducerProperties> properties = new ExtendedProducerProperties<>(
				new PulsarProducerProperties());
		properties.getExtension().setPartitionCount(4);
		ProducerDestination producerDestination = pulsarTopicProvisioner.provisionProducerDestination("foo",
				properties);
		verifyAndAssert(pulsarAdministration, producerDestination.getName(), "foo", 4);
	}

	@Test
	void provisionThroughConsumerBindingWithPartitionsSetAtTheBinderProperties() {
		PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties = new PulsarBinderConfigurationProperties();
		pulsarBinderConfigurationProperties.setPartitionCount(4);
		PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				pulsarBinderConfigurationProperties);
		ExtendedConsumerProperties<PulsarConsumerProperties> properties = new ExtendedConsumerProperties<>(
				new PulsarConsumerProperties());
		ConsumerDestination consumerDestination = pulsarTopicProvisioner.provisionConsumerDestination("bar", "",
				properties);
		verifyAndAssert(pulsarAdministration, consumerDestination.getName(), "bar", 4);
	}

	@Test
	void provisionThroughConsumerBindingWithPartitionsSetAtTheBindingProperties() {
		PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties = new PulsarBinderConfigurationProperties();
		PulsarAdministration pulsarAdministration = mock(PulsarAdministration.class);
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				pulsarBinderConfigurationProperties);
		PulsarConsumerProperties pulsarConsumerProperties = new PulsarConsumerProperties();
		pulsarConsumerProperties.setPartitionCount(4);
		ExtendedConsumerProperties<PulsarConsumerProperties> properties = new ExtendedConsumerProperties<>(
				pulsarConsumerProperties);
		ConsumerDestination consumerDestination = pulsarTopicProvisioner.provisionConsumerDestination("bar", "",
				properties);
		verifyAndAssert(pulsarAdministration, consumerDestination.getName(), "bar", 4);
	}

}
