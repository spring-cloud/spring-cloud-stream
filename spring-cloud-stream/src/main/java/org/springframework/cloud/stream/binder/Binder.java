/*
 * Copyright 2013-2016 the original author or authors.
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

import java.util.Properties;

/**
 * A strategy interface used to bind a module interface to a logical name. The name is intended to identify a
 * logical consumer or producer of messages. This may be a queue, a channel adapter, another message channel, a Spring
 * bean, etc.
 * @author Mark Fisher
 * @author David Turanski
 * @author Gary Russell
 * @author Jennifer Hickey
 * @author Ilayaperumal Gopinathan
 * @since 1.0
 */
public interface Binder<T> {

	/**
	 * Bind a message consumer on a channel
	 * @param name the logical identity of the message source
	 * @param group the consumer group to which this consumer belongs - subscriptions are shared among consumers
	 * in the same group (if <code>null</code> or empty String, the "default" group will be used)
	 * @param inboundBindTarget the module interface to be bound as a consumer
	 * @param properties arbitrary String key/value pairs that will be used in the binding
	 */
	void bindConsumer(String name, String group, T inboundBindTarget, Properties properties);

	/**
	 * Bind a message producer on a channel.
	 * @param name the logical identity of the message target
	 * @param outboundBindTarget the module interface bound as a producer
	 * @param properties arbitrary String key/value pairs that will be used in the binding
	 */
	void bindProducer(String name, T outboundBindTarget, Properties properties);

	/**
	 * Unbind inbound module components and stop any active components that use the channel
	 * with the supplied consumer group.
	 * @param name the channel name
	 * @param group the consumer group
	 */
	void unbindConsumers(String name, String group);

	/**
	 * Unbind outbound module components and stop any active components that use the channel.
	 * @param name the channel name
	 */
	void unbindProducers(String name);

	/**
	 * Unbind a specific message consumer
	 * @param name The logical identity of a message source
	 * @param group The consumer group
	 * @param inboundBindTarget The module interface bound as a consumer
	 */
	void unbindConsumer(String name, String group, T inboundBindTarget);

	/**
	 * Unbind a specific message producer
	 * @param name the logical identity of the message target
	 * @param outboundBindTarget the channel bound as a producer
	 */
	void unbindProducer(String name, T outboundBindTarget);

}
