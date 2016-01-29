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


import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.utils.MockBinderConfiguration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolutionException;

/**
 * @author Gary Russell
 * @author Mark Fisher
 * @author Marius Bogoevici
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
		Binding<MessageChannel> mockBinding = Binding.forConsumer("foo", null, Mockito.mock(AbstractEndpoint.class),
				inputChannel, null);
		when(binder.bindConsumer("foo", null, inputChannel, new Properties()))
				.thenReturn(mockBinding);
		Binding<MessageChannel> binding = service.bindConsumer(inputChannel, name);
		assertThat(binding, sameInstance(mockBinding));
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
		props.setGroup("fooGroup");
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
		Binding<MessageChannel> mockBinding = Binding.forConsumer("foo", "fooGroup", Mockito.mock(AbstractEndpoint.class),
				inputChannel, null);
		when(binder.bindConsumer("foo", "fooGroup", inputChannel, new Properties()))
				.thenReturn(mockBinding);
		Binding<MessageChannel> binding = service.bindConsumer(inputChannel, name);
		assertThat(binding, sameInstance(mockBinding));

		service.unbindConsumers(name);
		verify(binder).bindConsumer(name, props.getGroup(), inputChannel, properties.getConsumerProperties(name));
		verify(binder).unbind(binding);
		binderFactory.destroy();
	}

	@Test
	public void checkDynamicBinding () {

		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		DefaultBinderFactory<MessageChannel> binderFactory =
				new DefaultBinderFactory<>(Collections.singletonMap("mock",
						new BinderConfiguration(new BinderType("mock", new Class[]{MockBinderConfiguration.class}),
								new Properties(), true)));
		Binder<MessageChannel> binder = binderFactory.getBinder("mock");

		MessageChannel inputChannel = new DirectChannel();
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		Binding<MessageChannel> mockBinding = Binding.forConsumer("bar", null, Mockito.mock(AbstractEndpoint.class),
				inputChannel, null);

		final AtomicReference<MessageChannel> dynamic = new AtomicReference<>();
		when(binder.bindProducer(
				matches("mock:bar"), any(DirectChannel.class), any(Properties.class))).thenReturn(mockBinding);
		BinderAwareChannelResolver resolver = new BinderAwareChannelResolver(binderFactory, properties);
		ConfigurableListableBeanFactory beanFactory = mock(ConfigurableListableBeanFactory.class);
		when(beanFactory.getBean("mock:bar", MessageChannel.class))
				.thenThrow(new NoSuchBeanDefinitionException(MessageChannel.class));
		doAnswer(new Answer<Void>(){

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				dynamic.set(invocation.getArgumentAt(1, MessageChannel.class));
				return null;
			}

		}).when(beanFactory).registerSingleton(eq("mock:bar"), any(MessageChannel.class));
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return dynamic.get();
			}

		}).when(beanFactory).initializeBean(any(MessageChannel.class), eq("mock:bar"));
		resolver.setBeanFactory(beanFactory);
		MessageChannel resolved = resolver.resolveDestination("mock:bar");
		assertThat(resolved, sameInstance(dynamic.get()));
		verify(binder).bindProducer(eq("mock:bar"), eq(dynamic.get()), any(Properties.class));
		properties.setDynamicDestinations(new String[] { "mock:bar" });
		resolved = resolver.resolveDestination("mock:bar");
		assertThat(resolved, sameInstance(dynamic.get()));
		properties.setDynamicDestinations(new String[] { "foo:bar" });
		try {
			resolved = resolver.resolveDestination("mock:bar");
			fail();
		}
		catch (DestinationResolutionException e) {
			assertThat(e.getMessage(), containsString("Failed to find MessageChannel bean with name 'mock:bar'"));
		}
	}

}
