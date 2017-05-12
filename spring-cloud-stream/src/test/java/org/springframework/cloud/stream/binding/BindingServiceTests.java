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
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.reflection.GenericsUtils;
import org.springframework.cloud.stream.utils.MockBinderConfiguration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.matches;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Gary Russell
 * @author Mark Fisher
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class BindingServiceTests {

	@Test
	public void testDefaultGroup() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(Collections.singletonMap("mock",
				new BinderConfiguration(new BinderType("mock", new Class[] { MockBinderConfiguration.class }),
						new Properties(), true, true)));
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties, binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		when(binder.bindConsumer(eq("foo"), isNull(String.class), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding);
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				inputChannelName);
		assertThat(bindings).hasSize(1);
		Binding<MessageChannel> binding = bindings.iterator().next();
		assertThat(binding).isSameAs(mockBinding);
		service.unbindConsumers(inputChannelName);
		verify(binder).bindConsumer(eq("foo"), isNull(String.class), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding).unbind();
		binderFactory.destroy();
	}

	@Test
	public void testMultipleConsumerBindings() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo,bar");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);

		properties.setBindings(bindingProperties);

		DefaultBinderFactory binderFactory = new DefaultBinderFactory(Collections.singletonMap("mock",
				new BinderConfiguration(new BinderType("mock", new Class[] { MockBinderConfiguration.class }),
						new Properties(), true, true)));

		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();

		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding1 = Mockito.mock(Binding.class);
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding2 = Mockito.mock(Binding.class);

		when(binder.bindConsumer(eq("foo"), isNull(String.class), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding1);
		when(binder.bindConsumer(eq("bar"), isNull(String.class), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding2);

		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				"input");
		assertThat(bindings).hasSize(2);

		Iterator<Binding<MessageChannel>> iterator = bindings.iterator();
		Binding<MessageChannel> binding1 = iterator.next();
		Binding<MessageChannel> binding2 = iterator.next();

		assertThat(binding1).isSameAs(mockBinding1);
		assertThat(binding2).isSameAs(mockBinding2);

		service.unbindConsumers("input");

		verify(binder).bindConsumer(eq("foo"), isNull(String.class), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binder).bindConsumer(eq("bar"), isNull(String.class), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding1).unbind();
		verify(binding2).unbind();

		binderFactory.destroy();
	}

	@Test
	public void testExplicitGroup() throws Exception {
		BindingServiceProperties properties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		props.setDestination("foo");
		props.setGroup("fooGroup");
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		properties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(
				Collections
						.singletonMap("mock",
								new BinderConfiguration(
										new BinderType("mock",
												new Class[] {
														MockBinderConfiguration.class }),
										new Properties(), true, true)));
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		BindingService service = new BindingService(properties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		when(binder.bindConsumer(eq("foo"), eq("fooGroup"), same(inputChannel),
				any(ConsumerProperties.class))).thenReturn(mockBinding);
		Collection<Binding<MessageChannel>> bindings = service.bindConsumer(inputChannel,
				inputChannelName);
		assertThat(bindings).hasSize(1);
		Binding<MessageChannel> binding = bindings.iterator().next();
		assertThat(binding).isSameAs(mockBinding);

		service.unbindConsumers(inputChannelName);
		verify(binder).bindConsumer(eq("foo"), eq(props.getGroup()), same(inputChannel),
				any(ConsumerProperties.class));
		verify(binding).unbind();
		binderFactory.destroy();
	}

	@Test
	public void checkDynamicBinding() {
		BindingServiceProperties properties = new BindingServiceProperties();
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(
				Collections
						.singletonMap("mock",
								new BinderConfiguration(
										new BinderType("mock",
												new Class[] {
														MockBinderConfiguration.class }),
										new Properties(), true, true)));
		Binder binder = binderFactory.getBinder("mock", MessageChannel.class);
		@SuppressWarnings("unchecked")
		Binding<MessageChannel> mockBinding = Mockito.mock(Binding.class);
		@SuppressWarnings("unchecked")
		final AtomicReference<MessageChannel> dynamic = new AtomicReference<>();
		when(binder.bindProducer(matches("foo"), any(DirectChannel.class),
				any(ProducerProperties.class))).thenReturn(mockBinding);
		BindingService bindingService = new BindingService(properties, binderFactory);
		SubscribableChannelBindingTargetFactory bindableSubscribableChannelFactory = new SubscribableChannelBindingTargetFactory(
				new MessageConverterConfigurer(properties,
						new CompositeMessageConverterFactory()));
		BinderAwareChannelResolver resolver = new BinderAwareChannelResolver(
				bindingService, bindableSubscribableChannelFactory,
				new DynamicDestinationsBindable());
		ConfigurableListableBeanFactory beanFactory = mock(
				ConfigurableListableBeanFactory.class);
		when(beanFactory.getBean("foo", MessageChannel.class))
				.thenThrow(new NoSuchBeanDefinitionException(MessageChannel.class));
		when(beanFactory.getBean("bar", MessageChannel.class))
				.thenThrow(new NoSuchBeanDefinitionException(MessageChannel.class));
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				dynamic.set(invocation.getArgumentAt(1, MessageChannel.class));
				return null;
			}

		}).when(beanFactory).registerSingleton(eq("foo"), any(MessageChannel.class));
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				return dynamic.get();
			}

		}).when(beanFactory).initializeBean(any(MessageChannel.class), eq("foo"));
		resolver.setBeanFactory(beanFactory);
		MessageChannel resolved = resolver.resolveDestination("foo");
		assertThat(resolved).isSameAs(dynamic.get());
		verify(binder).bindProducer(eq("foo"), eq(dynamic.get()),
				any(ProducerProperties.class));
		properties.setDynamicDestinations(new String[] { "foo" });
		resolved = resolver.resolveDestination("foo");
		assertThat(resolved).isSameAs(dynamic.get());
		properties.setDynamicDestinations(new String[] { "test" });
		try {
			resolved = resolver.resolveDestination("bar");
			fail();
		}
		catch (DestinationResolutionException e) {
			assertThat(e).hasMessageContaining("Failed to find MessageChannel bean with name 'bar'");
		}
	}

	@Test
	public void testProducerPropertiesValidation() {
		BindingServiceProperties serviceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		ProducerProperties producerProperties = new ProducerProperties();
		producerProperties.setPartitionCount(0);
		props.setDestination("foo");
		props.setProducer(producerProperties);
		final String outputChannelName = "output";
		bindingProperties.put(outputChannelName, props);
		serviceProperties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(Collections.singletonMap("mock",
				new BinderConfiguration(new BinderType("mock", new Class[] { MockBinderConfiguration.class }),
						new Properties(), true, true)));
		BindingService service = new BindingService(serviceProperties, binderFactory);
		MessageChannel outputChannel = new DirectChannel();
		try {
			service.bindProducer(outputChannel, outputChannelName);
			fail("Producer properties should be validated.");
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Partition count should be greater than zero.");
		}
	}

	@Test
	public void testConsumerPropertiesValidation() {
		BindingServiceProperties serviceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindingProperties = new HashMap<>();
		BindingProperties props = new BindingProperties();
		ConsumerProperties consumerProperties = new ConsumerProperties();
		consumerProperties.setConcurrency(0);
		props.setDestination("foo");
		props.setConsumer(consumerProperties);
		final String inputChannelName = "input";
		bindingProperties.put(inputChannelName, props);
		serviceProperties.setBindings(bindingProperties);
		DefaultBinderFactory binderFactory = new DefaultBinderFactory(Collections.singletonMap("mock",
				new BinderConfiguration(new BinderType("mock", new Class[] { MockBinderConfiguration.class }),
						new Properties(), true, true)));
		BindingService service = new BindingService(serviceProperties,
				binderFactory);
		MessageChannel inputChannel = new DirectChannel();
		try {
			service.bindConsumer(inputChannel, inputChannelName);
			fail("Consumer properties should be validated.");
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("Concurrency should be greater than zero.");
		}
	}

	@Test
	public void testResolveBindableType() {
		Class<?> bindableType = GenericsUtils.getParameterType(FooBinder.class, Binder.class, 0);
		assertThat(bindableType).isSameAs(SomeBindableType.class);
	}

	public static class FooBinder
			implements Binder<SomeBindableType, ConsumerProperties, ProducerProperties> {
		@Override
		public Binding<SomeBindableType> bindConsumer(String name, String group,
				SomeBindableType inboundBindTarget,
				ConsumerProperties consumerProperties) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Binding<SomeBindableType> bindProducer(String name,
				SomeBindableType outboundBindTarget,
				ProducerProperties producerProperties) {
			throw new UnsupportedOperationException();
		}
	}

	public static class SomeBindableType {
	}
}
