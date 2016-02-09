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
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory<MessageChannel> binderFactory =
				new DefaultBinderFactory<>(Collections.singletonMap("mock",
						new BinderConfiguration(new BinderType("mock", new Class[]{MockBinderConfiguration.class}),
								new Properties(), true)));
		Binder<MessageChannel> binder = binderFactory.getBinder("mock");
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		when(binder.bindConsumer("foo", null, inputChannel, new Properties()))
				.thenReturn(mockBinding);
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel, inputChannelName);
		assertThat(bindings.size(), is(1));
		Binding<MessageChannel> binding = bindings.iterator().next();
		assertThat(binding, sameInstance(mockBinding));
		service.unbindConsumers(inputChannelName);
		verify(binder).bindConsumer("foo", props.getGroup(), inputChannel, properties.getConsumerProperties(inputChannelName));
		verify(binding).unbind();
		binderFactory.destroy();
	}

	@Test
	public void testMultipleConsumerBindings() throws Exception {
		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();

		BindingProperties props = new BindingProperties();
		props.setDestination("foo,bar");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);

		properties.setBindings(bindingProperties);

		DefaultBinderFactory<MessageChannel> binderFactory =
				new DefaultBinderFactory<>(Collections.singletonMap("mock",
						new BinderConfiguration(new BinderType("mock", new Class[]{MockBinderConfiguration.class}),
								new Properties(), true)));

		Binder<MessageChannel> binder = binderFactory.getBinder("mock");
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();

		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding1 = Mockito.mock(Binding.class);
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding2 = Mockito.mock(Binding.class);

		when(binder.bindConsumer("foo", null, inputChannel, new Properties()))
				.thenReturn(mockBinding1);
		when(binder.bindConsumer("bar", null, inputChannel, new Properties()))
				.thenReturn(mockBinding2);

		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel, "input");
		assertThat(bindings.size(), is(2));

		Iterator<Binding<MessageChannel>> iterator = bindings.iterator();
		Binding<MessageChannel> binding1 = iterator.next();
		Binding<MessageChannel> binding2 = iterator.next();

		assertThat(binding1, sameInstance(mockBinding1));
		assertThat(binding2, sameInstance(mockBinding2));

		service.unbindConsumers("input");

		verify(binder).bindConsumer("foo", props.getGroup(), inputChannel, properties.getConsumerProperties(inputChannelName));
		verify(binder).bindConsumer("bar", props.getGroup(), inputChannel, properties.getConsumerProperties(inputChannelName));
		verify(binding1).unbind();
		verify(binding2).unbind();

		binderFactory.destroy();
	}

	@Test
	public void testExplicitGroup() throws Exception {
		ChannelBindingServiceProperties properties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		props.setGroup("fooGroup");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory<MessageChannel> binderFactory =
				new DefaultBinderFactory<>(Collections.singletonMap("mock",
						new BinderConfiguration(new BinderType("mock", new Class[]{MockBinderConfiguration.class}),
								new Properties(), true)));
		Binder<MessageChannel> binder = binderFactory.getBinder("mock");
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		when(binder.bindConsumer("foo", "fooGroup", inputChannel, new Properties()))
				.thenReturn(mockBinding);
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel, inputChannelName);
		assertThat(bindings.size(), is(1));
		Binding<MessageChannel> binding = bindings.iterator().next();
		assertThat(binding, sameInstance(mockBinding));

		service.unbindConsumers(inputChannelName);
		verify(binder).bindConsumer("foo", props.getGroup(), inputChannel, properties.getConsumerProperties(inputChannelName));
		verify(binding).unbind();
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
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		ChannelBindingService service = new ChannelBindingService(properties, binderFactory);
		@SuppressWarnings("unchecked")
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
