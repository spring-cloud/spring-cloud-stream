/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.SubscribableChannel;

/**
 * {@link ProvisioningProvider} to support {@link TestChannelBinder}. It exists primarily
 * to support {@link AbstractMessageChannel} semantics for creating
 * {@link ConsumerDestination} and {@link ProducerDestination}, to interact with this
 * {@link Binder}.
 *
 * @author Oleg Zhurakousky
 *
 */
public class TestChannelBinderProvisioner
	implements ProvisioningProvider<ConsumerProperties, ProducerProperties> {

	private final Map<String, SubscribableChannel> provisionedDestinations = new HashMap<>();

	@Autowired
	private InputDestination source;

	@Autowired
	private OutputDestination target;

	/**
	 * Will provision producer destination as an SI {@link PublishSubscribeChannel}. <br>
	 * This provides convenience of registering additional subscriber (handler in the test
	 * method) along side of being able to call {@link OutputDestination#receive()} to get
	 * a {@link Message} for additional assertions.
	 */
	@Override
	public ProducerDestination provisionProducerDestination(String name,
															ProducerProperties properties) throws ProvisioningException {
		SubscribableChannel destination = this.provisionDestination(name, true);
		this.target.setChannel(destination);
		return new SpringIntegrationProducerDestination(name, destination);
	}

	/**
	 * Will provision consumer destination as SI {@link DirectChannel}.
	 */
	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
															ConsumerProperties properties) throws ProvisioningException {
		SubscribableChannel destination = this.provisionDestination(name, false);
		if (this.source != null) {
			this.source.setChannel(destination);
		}
		return new SpringIntegrationConsumerDestination(name, destination);
	}

	private SubscribableChannel provisionDestination(String name, boolean pubSub) {
		String destinationName = name + ".destination";
		SubscribableChannel destination = this.provisionedDestinations
			.get(destinationName);
		if (destination == null) {
			destination = new PublishSubscribeChannel();
			((AbstractMessageChannel) destination).setBeanName(destinationName);
			((AbstractMessageChannel) destination).setComponentName(destinationName);
			this.provisionedDestinations.put(destinationName, destination);
		}
		return destination;
	}

	class SpringIntegrationConsumerDestination implements ConsumerDestination {

		private final String name;

		private final SubscribableChannel channel;

		SpringIntegrationConsumerDestination(String name, SubscribableChannel channel) {
			this.name = name;
			this.channel = channel;
		}

		public SubscribableChannel getChannel() {
			return this.channel;
		}

		@Override
		public String getName() {
			return this.name;
		}

	}

	class SpringIntegrationProducerDestination implements ProducerDestination {

		private final String name;

		private final SubscribableChannel channel;

		SpringIntegrationProducerDestination(String name, SubscribableChannel channel) {
			this.name = name;
			this.channel = channel;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.getName() + partition;
		}

		public SubscribableChannel getChannel() {
			return this.channel;
		}

		@Override
		public String getName() {
			return this.name;
		}

	}

}
