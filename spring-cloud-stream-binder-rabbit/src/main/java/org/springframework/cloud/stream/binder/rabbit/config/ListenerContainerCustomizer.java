/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;

/**
 * If a single bean of this type is in the application context, listener
 * containers created by the binder can be further customized after all
 * the properties are set. For example, to configure less-common
 * properties.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
@FunctionalInterface
public interface ListenerContainerCustomizer {

	/**
	 * Configure the container that is being created for the supplied queue name and
	 * consumer group.
	 * @param container the container.
	 * @param queueName the destination name.
	 * @param group the consumer group.
	 */
	void configure(AbstractMessageListenerContainer container, String queueName, String group);

}
