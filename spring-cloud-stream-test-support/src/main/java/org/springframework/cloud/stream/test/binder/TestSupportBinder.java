/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.cloud.stream.binder.AbstractBinderPropertiesAccessor;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * A minimal binder that<ul>
 *     <li>does nothing about binding consumers, leaving the channel as-is, so that a test author can interact with it directly,</li>
 *     <li>registers a queue channel on the producer side, so that it is easy to assert what is received.</li>
 * </ul>
 *
 * @author Eric Bottard
 * @see org.springframework.cloud.stream.test.matcher.MessageChannelMatcher
 */
public class TestSupportBinder extends MessageChannelBinderSupport {

	public static Map<MessageChannel, QueueChannel> channelToBindings =
			new HashMap<>();


	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		// Do nothing. A module author can grab hold of the input channel
		// and interact with it directly
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {

	}

	/**
	 * Bridges a QueueChannel to the module output channel, so that it can
	 * be easily queried by {@link org.springframework.cloud.stream.test.matcher.MessageChannelMatcher}.
	 */
	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		BridgeHandler bridge = new BridgeHandler();
		QueueChannel queue = new QueueChannel();
		bridge.setOutputChannel(queue);
		AbstractEndpoint ae = new EventDrivenConsumer((SubscribableChannel) outboundBindTarget, bridge);
		ae.setBeanName("outbound." + name);
		Binding binding = Binding.forConsumer(name, ae, outboundBindTarget, new NullPropertiesAccessor(properties));
		addBinding(binding);
		channelToBindings.put(outboundBindTarget, queue);
		binding.start();
	}

	@Override
	public void unbindProducer(String name, MessageChannel channel) {
		super.unbindProducer(name, channel);
		channelToBindings.remove(channel);
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {

	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {

	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {

	}


	private static class NullPropertiesAccessor extends AbstractBinderPropertiesAccessor {

		public NullPropertiesAccessor(Properties properties) {
			super(properties);
		}
	}
}
