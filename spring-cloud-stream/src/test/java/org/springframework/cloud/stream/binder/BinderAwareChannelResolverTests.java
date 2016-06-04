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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.binding.BindableChannelFactory;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.ChannelBindingService;
import org.springframework.cloud.stream.binding.DefaultBindableChannelFactory;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public class BinderAwareChannelResolverTests {

	protected final StaticApplicationContext context = new StaticApplicationContext();

	protected volatile BinderAwareChannelResolver resolver;

	protected volatile Binder<MessageChannel, ConsumerProperties, ProducerProperties> binder;

	protected volatile BindableChannelFactory bindableChannelFactory;

	protected volatile ChannelBindingServiceProperties channelBindingServiceProperties;

	protected volatile DynamicDestinationsBindable dynamicDestinationsBindable;

	private volatile List<TestBinder.TestBinding> producerBindings;

	@Before
	public void setupContext() throws Exception {
		this.producerBindings = new ArrayList<>();
		this.binder = new TestBinder();
		BinderFactory binderFactory = new BinderFactory<MessageChannel>() {

			@Override
			public Binder<MessageChannel, ConsumerProperties, ProducerProperties> getBinder(
					String configurationName) {
				return BinderAwareChannelResolverTests.this.binder;
			}
		};
		this.channelBindingServiceProperties = new ChannelBindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setContentType("text/plain");
		bindings.put("foo", bindingProperties);
		this.channelBindingServiceProperties.setBindings(bindings);
		ChannelBindingService channelBindingService = new ChannelBindingService(
				this.channelBindingServiceProperties, binderFactory);
		MessageConverterConfigurer messageConverterConfigurer = new MessageConverterConfigurer(
				this.channelBindingServiceProperties, new DefaultMessageBuilderFactory(),
				new CompositeMessageConverterFactory());
		messageConverterConfigurer
				.setBeanFactory(Mockito.mock(ConfigurableListableBeanFactory.class));
		messageConverterConfigurer.afterPropertiesSet();
		this.bindableChannelFactory = new DefaultBindableChannelFactory(
				messageConverterConfigurer);
		this.dynamicDestinationsBindable = new DynamicDestinationsBindable();
		this.resolver = new BinderAwareChannelResolver(channelBindingService,
				this.bindableChannelFactory, this.dynamicDestinationsBindable);
		this.resolver.setBeanFactory(this.context.getBeanFactory());
		this.context.getBeanFactory().registerSingleton("channelResolver", this.resolver);
		this.context.getBeanFactory().registerSingleton("dynamicDestinationBindable",
				this.dynamicDestinationsBindable);
		this.context.registerSingleton("other", DirectChannel.class);
		this.context.registerSingleton(
				IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				DefaultMessageBuilderFactory.class);
		this.context.getBeanFactory().registerSingleton("channelBindingService",
				channelBindingService);
		this.context.registerSingleton("inputBindingLifecycle",
				InputBindingLifecycle.class);
		this.context.registerSingleton("outputBindingLifecycle",
				OutputBindingLifecycle.class);
		this.context.refresh();
	}

	@Test
	public void resolveChannel() {
		assertThat(this.producerBindings).hasSize(0);
		MessageChannel registered = this.resolver.resolveDestination("foo");
		assertThat(this.producerBindings).hasSize(1);
		TestBinder.TestBinding binding = this.producerBindings.get(0);
		assertThat(binding.isBound()).describedAs("Must be bound");
		DirectChannel testChannel = new DirectChannel();
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message<?>> received = new ArrayList<>();
		testChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				received.add(message);
				latch.countDown();
			}
		});
		this.binder.bindConsumer("foo", null, testChannel, new ConsumerProperties());
		assertThat(received).hasSize(0);
		registered.send(MessageBuilder.withPayload("hello").build());
		try {
			assertThat(latch.await(1, TimeUnit.SECONDS)).describedAs("Latch timed out");
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			fail("interrupted while awaiting latch");
		}
		assertThat(received).hasSize(1);
		assertThat(received.get(0).getPayload()).isEqualTo("hello");
		this.context.close();
		assertThat(this.producerBindings).hasSize(1);
		assertThat(binding.isBound()).isFalse().describedAs("Must not be bound");
	}

	@Test
	public void resolveNonRegisteredChannel() {
		MessageChannel other = this.resolver.resolveDestination("other");
		assertThat(this.context.getBean("other")).isSameAs(other);
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void propertyPassthrough() {
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties genericProperties = new BindingProperties();
		genericProperties.setContentType("text/plain");
		bindings.put("foo", genericProperties);
		this.channelBindingServiceProperties.setBindings(bindings);
		@SuppressWarnings("unchecked")
		Binder binder = mock(Binder.class);
		Binder binder2 = mock(Binder.class);
		BinderFactory<MessageChannel> mockBinderFactory = Mockito
				.mock(BinderFactory.class);
		Binding<MessageChannel> fooBinding = Mockito.mock(Binding.class);
		Binding<MessageChannel> barBinding = Mockito.mock(Binding.class);
		when(binder.bindProducer(matches("foo"), any(DirectChannel.class),
				any(ProducerProperties.class))).thenReturn(fooBinding);
		when(binder2.bindProducer(matches("bar"), any(DirectChannel.class),
				any(ProducerProperties.class))).thenReturn(barBinding);
		when(mockBinderFactory.getBinder(null)).thenReturn(binder);
		when(mockBinderFactory.getBinder("someTransport")).thenReturn(binder2);
		ChannelBindingService channelBindingService = new ChannelBindingService(
				this.channelBindingServiceProperties, mockBinderFactory);
		@SuppressWarnings("unchecked")
		BinderAwareChannelResolver resolver = new BinderAwareChannelResolver(
				channelBindingService, this.bindableChannelFactory,
				new DynamicDestinationsBindable());
		BeanFactory beanFactory = new DefaultListableBeanFactory();
		resolver.setBeanFactory(beanFactory);
		SubscribableChannel resolved = (SubscribableChannel) resolver
				.resolveDestination("foo");
		DirectFieldAccessor accessor = new DirectFieldAccessor(resolved);
		Class<?>[] dataTypes = (Class<?>[]) accessor.getPropertyValue("datatypes");
		Assert.isTrue(dataTypes.length == 1, "Data type must be set for the Foo Channel");
		Assert.isTrue(dataTypes[0].equals(String.class),
				"Data type should be of type String");
		verify(binder).bindProducer(eq("foo"), any(MessageChannel.class),
				any(ProducerProperties.class));
		assertThat(resolved).isSameAs(beanFactory.getBean("foo"));
	}

	/**
	 * A simple test binder that creates queues for the destinations. Ignores groups.
	 */
	private class TestBinder
			implements Binder<MessageChannel, ConsumerProperties, ProducerProperties> {

		private final Map<String, DirectChannel> destinations = new ConcurrentHashMap<>();

		@Override
		public Binding<MessageChannel> bindConsumer(String name, String group,
				MessageChannel inboundBindTarget, ConsumerProperties properties) {
			synchronized (this.destinations) {
				if (!this.destinations.containsKey(name)) {
					this.destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(inboundBindTarget);
			this.destinations.get(name).subscribe(directHandler);
			return new TestBinding(name, directHandler);
		}

		@Override
		public Binding<MessageChannel> bindProducer(String name,
				MessageChannel outboundBindTarget, ProducerProperties properties) {
			synchronized (this.destinations) {
				if (!this.destinations.containsKey(name)) {
					this.destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(this.destinations.get(name));
			// for test purposes we can assume it is a SubscribableChannel
			((SubscribableChannel) outboundBindTarget).subscribe(directHandler);
			TestBinding binding = new TestBinding(name, directHandler);
			BinderAwareChannelResolverTests.this.producerBindings.add(binding);
			return binding;
		}

		private final class TestBinding implements Binding<MessageChannel> {

			private final String name;

			private final DirectHandler directHandler;

			private boolean bound = true;

			private TestBinding(String name, DirectHandler directHandler) {
				this.name = name;
				this.directHandler = directHandler;
			}

			@Override
			public void unbind() {
				this.bound = false;
				TestBinder.this.destinations.get(this.name)
						.unsubscribe(this.directHandler);
			}

			public boolean isBound() {
				return this.bound;
			}
		}
	}
}
