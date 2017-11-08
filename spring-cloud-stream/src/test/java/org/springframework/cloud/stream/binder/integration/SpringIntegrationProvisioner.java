/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.integration;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.AbstractNamedDestination;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * {@link ProvisioningProvider} to support {@link SpringIntegrationChannelBinder}. It
 * exists primarily to support {@link AbstractMessageChannel} semantics for creating
 * {@link ConsumerDestination} and {@link ProducerDestination}, thus it returns SI
 * specific destinations which contain {@link MessageChannel}s used to interact with this
 * {@link Binder}
 *
 * @author Oleg Zhurakousky
 *
 */
public class SpringIntegrationProvisioner implements ProvisioningProvider<ConsumerProperties, ProducerProperties> {

	private Map<String, SubscribableChannel> provisionedDestinations = new HashMap<>();

	@Autowired
	private SourceDestination source;

	@Autowired
	private TargetDestination target;

	@Override
	public ProducerDestination provisionProducerDestination(String name, ProducerProperties properties) throws ProvisioningException {
		SubscribableChannel destination = this.provisionDestination(name);
		this.target.setChannel(destination);
		return new SpringIntegrationProducerDestination(name, destination);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group, ConsumerProperties properties) throws ProvisioningException {
		SubscribableChannel destination = this.provisionDestination(name);
		this.source.setChannel(destination);
		return new SpringIntegrationConsumerDestination(name, destination);
	}

	private SubscribableChannel provisionDestination(String name) {
		String destinationName = name + ".destination";
		SubscribableChannel destination = this.provisionedDestinations.get(destinationName);
		if (destination == null) {
			destination = new DirectChannel();
			((DirectChannel)destination).setBeanName(destinationName);
			((DirectChannel)destination).setComponentName(destinationName);
			this.provisionedDestinations.put(destinationName, destination);
		}
		return destination;
	}

	class SpringIntegrationConsumerDestination extends AbstractNamedDestination implements ConsumerDestination {
		private final SubscribableChannel channel;

		SpringIntegrationConsumerDestination(String name, SubscribableChannel channel) {
			super(name);
			this.channel = channel;
		}

		public SubscribableChannel getChannel() {
			return this.channel;
		}
	}

	class SpringIntegrationProducerDestination extends AbstractNamedDestination implements ProducerDestination {
		private final SubscribableChannel channel;

		SpringIntegrationProducerDestination(String name, SubscribableChannel channel) {
			super(name);
			this.channel = channel;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.getName() + partition;
		}

		public SubscribableChannel getChannel() {
			return this.channel;
		}
	}
}
