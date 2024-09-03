/*
 * Copyright 2022-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar.provisioning;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTopicBuilder;

/**
 * Pulsar topic provisioner.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarTopicProvisioner implements
		ProvisioningProvider<ExtendedConsumerProperties<PulsarConsumerProperties>, ExtendedProducerProperties<PulsarProducerProperties>> {

	private final PulsarAdministration pulsarAdministration;

	private final PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties;

	private final PulsarTopicBuilder topicBuilder;

	public PulsarTopicProvisioner(PulsarAdministration pulsarAdministration,
			PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties) {
		this(pulsarAdministration, pulsarBinderConfigurationProperties, new PulsarTopicBuilder());
	}

	public PulsarTopicProvisioner(PulsarAdministration pulsarAdministration,
			PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties,
			PulsarTopicBuilder topicBuilder) {
		this.pulsarAdministration = pulsarAdministration;
		this.pulsarBinderConfigurationProperties = pulsarBinderConfigurationProperties;
		this.topicBuilder = topicBuilder;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ExtendedProducerProperties<PulsarProducerProperties> pulsarProducerProperties)
			throws ProvisioningException {
		Integer partitionCountFromBinding = pulsarProducerProperties.getExtension().getPartitionCount();
		var partitionCount = getPartitionCount(partitionCountFromBinding);
		var pulsarTopic = this.topicBuilder.name(name).numberOfPartitions(partitionCount).build();
		this.pulsarAdministration.createOrModifyTopics(pulsarTopic);
		return new PulsarDestination(pulsarTopic.topicName(), pulsarTopic.numberOfPartitions());
	}

	private int getPartitionCount(@Nullable Integer partitionCountConfig) {
		var partitionCount = this.pulsarBinderConfigurationProperties.getPartitionCount();
		if (partitionCountConfig != null && partitionCountConfig > 0) {
			partitionCount = partitionCountConfig;
		}
		return partitionCount == null ? 0 : partitionCount;
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<PulsarConsumerProperties> pulsarConsumerProperties)
			throws ProvisioningException {
		var partitionCountFromBinding = pulsarConsumerProperties.getExtension().getPartitionCount();
		var partitionCount = getPartitionCount(partitionCountFromBinding);
		var pulsarTopic = this.topicBuilder.name(name).numberOfPartitions(partitionCount).build();
		this.pulsarAdministration.createOrModifyTopics(pulsarTopic);
		return new PulsarDestination(pulsarTopic.topicName(), pulsarTopic.numberOfPartitions());
	}

	private record PulsarDestination(String destinationName,
			Integer partitions) implements ProducerDestination, ConsumerDestination {

		@Override
		public String getName() {
			return this.destinationName;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.destinationName;
		}
	}

}
