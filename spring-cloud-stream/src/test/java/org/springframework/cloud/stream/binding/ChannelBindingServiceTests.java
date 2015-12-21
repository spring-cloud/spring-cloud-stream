/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.utils.MockBinderConfiguration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

/**
 * @author Gary Russell
 * @author Mark Fisher
 */
public class ChannelBindingServiceTests {

	@Test
	public void testDefaultGroup() throws Exception {
		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		String name = "foo";
		bindings.put(name, props);
		properties.setBindings(bindings);
		DefaultBinderFactory<MessageChannel> binderFactory =
				new DefaultBinderFactory<>(Collections.singletonMap("mock",
						new BinderConfiguration(new BinderType("mock", new Class[]{MockBinderConfiguration.class}),
								new Properties(), true)));
		Binder<MessageChannel> binder = binderFactory.getBinder("mock");
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		Binding<MessageChannel> binding = service.bindConsumer(inputChannel, name);
		service.unbindConsumers(name);
		verify(binder).bindConsumer(name, props.getGroup(), inputChannel, properties.getConsumerProperties(name));
		verify(binder).unbind(binding);
		binderFactory.destroy();
	}

	@Test
	public void testExplicitGroup() throws Exception {
		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		String name = "foo";
		bindings.put(name, props);
		properties.setBindings(bindings);
		DefaultBinderFactory<MessageChannel> binderFactory =
				new DefaultBinderFactory<>(Collections.singletonMap("mock",
						new BinderConfiguration(new BinderType("mock", new Class[]{MockBinderConfiguration.class}),
								new Properties(), true)));
		Binder<MessageChannel> binder = binderFactory.getBinder("mock");
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		Binding<MessageChannel> binding = service.bindConsumer(inputChannel, name);
		service.unbindConsumers(name);
		verify(binder).bindConsumer(name, props.getGroup(), inputChannel, properties.getConsumerProperties(name));
		verify(binder).unbind(binding);
		binderFactory.destroy();
	}

}
