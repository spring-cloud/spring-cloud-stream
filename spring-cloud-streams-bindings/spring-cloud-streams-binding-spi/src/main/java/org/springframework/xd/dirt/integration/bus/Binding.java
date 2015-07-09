/*
 * Copyright 2013-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;

import org.springframework.context.Lifecycle;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

/**
 * Represents a binding between a module's channel and an adapter endpoint that connects to the MessageBus. The binding
 * could be for a consumer or a producer. A consumer binding represents a connection from an adapter on the bus to a
 * module's input channel. A producer binding represents a connection from a module's output channel to an adapter on
 * the bus.
 *
 * @author Jennifer Hickey
 * @author Mark Fisher
 * @author Gary Russell
 */
public class Binding implements Lifecycle {

	public static final String PRODUCER = "producer";

	public static final String CONSUMER = "consumer";

	public static final String DIRECT = "direct";

	private final String name;

	private final MessageChannel channel;

	private final AbstractEndpoint endpoint;

	private final String type;

	private final AbstractBusPropertiesAccessor properties;

	private Binding(String name, MessageChannel channel, AbstractEndpoint endpoint, String type,
			AbstractBusPropertiesAccessor properties) {
		Assert.notNull(channel, "channel must not be null");
		Assert.notNull(endpoint, "endpoint must not be null");
		this.name = name;
		this.channel = channel;
		this.endpoint = endpoint;
		this.type = type;
		this.properties = properties;
	}

	public static Binding forConsumer(String name, AbstractEndpoint adapterFromBus, MessageChannel moduleInputChannel,
			AbstractBusPropertiesAccessor properties) {
		return new Binding(name, moduleInputChannel, adapterFromBus, CONSUMER, properties);
	}

	public static Binding forProducer(String name, MessageChannel moduleOutputChannel, AbstractEndpoint adapterToBus,
			AbstractBusPropertiesAccessor properties) {
		return new Binding(name, moduleOutputChannel, adapterToBus, PRODUCER, properties);
	}

	public static Binding forDirectProducer(String name, MessageChannel moduleOutputChannel,
			AbstractEndpoint adapter, AbstractBusPropertiesAccessor properties) {
		return new Binding(name, moduleOutputChannel, adapter, DIRECT, properties);
	}

	public String getName() {
		return name;
	}

	public MessageChannel getChannel() {
		return channel;
	}

	public AbstractEndpoint getEndpoint() {
		return endpoint;
	}

	public String getType() {
		return type;
	}

	public AbstractBusPropertiesAccessor getPropertiesAccessor() {
		return properties;
	}

	@Override
	public void start() {
		endpoint.start();
	}

	@Override
	public void stop() {
		endpoint.stop();
	}

	@Override
	public boolean isRunning() {
		return endpoint.isRunning();
	}

	@Override
	public String toString() {
		return type + " Binding [name=" + name + ", channel=" + channel + ", endpoint=" + endpoint.getComponentName()
				+ "]";
	}

}
