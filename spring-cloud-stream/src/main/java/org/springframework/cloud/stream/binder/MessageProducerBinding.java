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

package org.springframework.cloud.stream.binder;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;

/**
 * Implementation of {@link ConsumerBinding} used by {@link AbstractMessageChannelBinder} implementations.
 * Uses the lifecycle hook of {@link Binding} to initialize the {@link MessageProducer} returned by {@link AbstractMessageChannelBinder#createConsumerEndpoint(ConsumerDestination, String, ConsumerProperties)}
 *
 * @author Vinicius Carvalho
 */
public class MessageProducerBinding extends ConsumerBinding<MessageChannel> {

	private MessageProducer messageProducer;

	/**
	 * Creates an instance that associates a given name, group and binding target with an
	 * optional {@link Lifecycle} component, which will be stopped during unbinding.
	 * @param name
	the name of the binding target
	 * @param group the group (only for input targets)
	 * @param target the binding target
	 * @param lifecycle {@link Lifecycle} that runs while the binding is active and will
	 * @param provisioningDestination the provisioned destination created for this binding
	}
	 */
	public MessageProducerBinding(String name, String group, MessageChannel target, Lifecycle lifecycle, ConsumerDestination
			provisioningDestination) {
		super(name, group, target, lifecycle, provisioningDestination);
	}

	public MessageProducer getMessageProducer() {
		return this.messageProducer;
	}

	public void setMessageProducer(MessageProducer messageProducer) {
		this.messageProducer = messageProducer;
	}

	@Override
	/**
	 * Initializes the {@link MessageProducer} instance associated with this instance.
	 */
	protected void afterBind() throws Exception{
		if (messageProducer instanceof InitializingBean) {
			((InitializingBean) this.messageProducer).afterPropertiesSet();
		}
		if (messageProducer instanceof Lifecycle) {
			((Lifecycle) this.messageProducer).start();
		}
	}
}
