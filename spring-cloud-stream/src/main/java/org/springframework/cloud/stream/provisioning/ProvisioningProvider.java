/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.provisioning;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;

/**
 * Provisioning SPI that allows the users to provision destinations such as queues and
 * topics. This SPI will allow the binders to be separated from any provisioning concerns
 * and only focus on setting up endpoints for sending/receiving messages.
 *
 * Implementations must implement the following methods:
 *
 * <ul>
 * <li>{@link #provisionProducerDestination(String, ProducerProperties)}</li>
 * <li>{@link #provisionConsumerDestination(String, String, ConsumerProperties)}</li>
 * </ul>
 *
 * @param <C> the consumer properties type
 * @param <P> the producer properties type
 * @author Soby Chacko
 * @since 1.2
 */
public interface ProvisioningProvider<C extends ConsumerProperties, P extends ProducerProperties> {

	/**
	 * Creates middleware destination on the physical broker for the producer to send
	 * data. The implementation is middleware-specific.
	 * @param name the name of the producer destination
	 * @param properties producer properties
	 * @return reference to {@link ProducerDestination} that represents a producer
	 * @throws ProvisioningException on underlying provisioning errors from the middleware
	 */
	ProducerDestination provisionProducerDestination(String name, P properties)
			throws ProvisioningException;

	/**
	 * Creates the middleware destination on the physical broker for the consumer to
	 * consume data. The implementation is middleware-specific.
	 * @param name the name of the destination
	 * @param group the consumer group
	 * @param properties consumer properties
	 * @return reference to {@link ConsumerDestination} that represents a consumer
	 * @throws ProvisioningException on underlying provisioning errors from the middleware
	 */
	ConsumerDestination provisionConsumerDestination(String name, String group,
			C properties) throws ProvisioningException;

}
