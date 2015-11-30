/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.cloud.stream.binding;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

/**
 * @author Gary Russell
 *
 */
public class ChannelBindingServiceTests {

	@Test
	public void testSimple() {
		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		String name = "foo";
		bindings.put(name, props);
		properties.setBindings(bindings);
		@SuppressWarnings("unchecked")
		Binder<MessageChannel> binder = mock(Binder.class);
		ChannelBindingService service = new ChannelBindingService(properties, binder);
		MessageChannel inputChannel = new DirectChannel();
		service.bindConsumer(inputChannel, name);
		service.unbindConsumers(name);
		verify(binder).bindConsumer(name, inputChannel, properties.getConsumerProperties(name));
		verify(binder).unbindConsumers(name);
	}

	@Test
	public void testPubSub() {
		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("topic:foo");
		String name = "foo";
		bindings.put(name, props);
		properties.setBindings(bindings);
		@SuppressWarnings("unchecked")
		Binder<MessageChannel> binder = mock(Binder.class);
		ChannelBindingService service = new ChannelBindingService(properties, binder);
		MessageChannel inputChannel = new DirectChannel();
		service.bindConsumer(inputChannel, name);
		service.unbindConsumers(name);
		verify(binder).bindPubSubConsumer(name, inputChannel, props.getGroup(), properties.getConsumerProperties(name));
		verify(binder).unbindPubSubConsumers(name, props.getGroup());
	}

}
