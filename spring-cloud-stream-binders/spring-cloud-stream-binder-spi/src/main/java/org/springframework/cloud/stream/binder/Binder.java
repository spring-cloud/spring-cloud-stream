/*
 * Copyright 2013-2014 the original author or authors.
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
	 * Bind a message consumer on a p2p channel
	 * @param name the logical identity of the message source
	 * @param inboundBindTarget the module interface to be bound as a point to point consumer
	 * @param properties arbitrary String key/value pairs that will be used in the binding
	 */
	void bindConsumer(String name, T inboundBindTarget, Properties properties);


	/**
	 * Bind a message consumer on a pub/sub channel
	 * @param name the logical identity of the message source
	 * @param inboundBindTarget the module interface to be bound as a pub/sub consumer
	 * @param properties arbitrary String key/value pairs that will be used in the binding
	 */
	void bindPubSubConsumer(final String name, T inboundBindTarget, Properties properties);

	/**
	 * Bind a message producer on a p2p channel.
	 * @param name the logical identity of the message target
	 * @param outboundBindTarget the module interface bound as a producer
	 * @param properties arbitrary String key/value pairs that will be used in the binding
	 */
	void bindProducer(String name, T outboundBindTarget, Properties properties);


	/**
	 * Bind a message producer on a pub/sub channel.
	 * @param name the logical identity of the message target
	 * @param outboundBindTarget the module interface bound as a producer
	 * @param properties arbitrary String key/value pairs that will be used in the binding
	 */
	void bindPubSubProducer(final String name, T outboundBindTarget, Properties properties);

	/**
	 * Unbind inbound module components and stop any active components that use the channel.
	 * @param name the channel name
	 */
	void unbindConsumers(String name);

	/**
	 * Unbind outbound module components and stop any active components that use the channel.
	 * @param name the channel name
	 */
	void unbindProducers(String name);

	/**
	 * Unbind a specific p2p or pub/sub message consumer
	 * @param name The logical identify of a message source
	 * @param inboundBindTarget The module interface bound as a consumer
	 */
	void unbindConsumer(String name, T inboundBindTarget);

	/**
	 * Unbind a specific p2p or pub/sub message producer
	 * @param name the logical identity of the message target
	 * @param outboundBindTarget the channel bound as a producer
	 */
	void unbindProducer(String name, T outboundBindTarget);

	/**
	 * Bind a producer that expects async replies. To unbind, invoke unbindProducer() and unbindConsumer().
	 * @param name The name of the requestor.
	 * @param requests The interface used to send requests.
	 * @param replies The interface used to receive replies.
	 * @param properties arbitrary String key/value pairs that will be used in the binding.
	 */
	void bindRequestor(String name, T requests, T replies, Properties properties);

	/**
	 * Bind a consumer that handles requests from a requestor and asynchronously sends replies. To unbind, invoke
	 * unbindProducer() and unbindConsumer().
	 * @param name The name of the requestor for which this replier will handle requests.
	 * @param requests The interface used to send requests.
	 * @param replies The interface used to receive replies.
	 * @param properties arbitrary String key/value pairs that will be used in the binding.
	 */
	void bindReplier(String name, T requests, T replies, Properties properties);

	/**
	 * Create an object and bind a producer dynamically, creating the infrastructure
	 * required by the binder technology.
	 * @param name The name of the "queue:" channel.
	 * @param properties arbitrary String key/value pairs that will be used in the binding.
	 * @return The bound object.
	 */
	T bindDynamicProducer(String name, Properties properties);

	/**
	 * Create an object and bind a producer dynamically, creating the infrastructure
	 * required by the binder technology to broadcast messages to consumers.
	 * @param name The name of the "topic:" channel.
	 * @param properties arbitrary String key/value pairs that will be used in the binding.
	 * @return The bound Object.
	 */
	T bindDynamicPubSubProducer(String name, Properties properties);

	/**
	 * Return true if the binder supports the capability.
	 * @param capability the capability.
	 * @return true if the capability is supported.
	 */
	boolean isCapable(Capability capability);

	public enum Capability {

		/**
		 * When a binder supports durable subscriptions to a pub/sub channel, the stream
		 * name will be included in the consumer name.
		 */
		DURABLE_PUBSUB

	}

}
