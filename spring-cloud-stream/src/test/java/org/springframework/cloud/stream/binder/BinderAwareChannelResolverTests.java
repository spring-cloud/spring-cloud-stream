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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.DynamicDestinationsBindable;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public class BinderAwareChannelResolverTests {

	protected final StaticApplicationContext context = new StaticApplicationContext();

	protected volatile BinderAwareChannelResolver resolver;

	protected volatile Binder<MessageChannel, ConsumerProperties, ProducerProperties> binder;

	protected volatile AbstractBindingTargetFactory<? extends MessageChannel> bindingTargetFactory;

	protected volatile BindingServiceProperties bindingServiceProperties;

	protected volatile DynamicDestinationsBindable dynamicDestinationsBindable;

	private volatile List<TestBinder.TestBinding> producerBindings;

	@Before
	public void setupContext() throws Exception {
		producerBindings = new ArrayList<>();
		this.binder = new TestBinder();
		BinderFactory binderFactory = new BinderFactory() {

			@Override
			public <T> Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties> getBinder(
					String configurationName, Class<? extends T> bindableType) {
				return (Binder<T, ? extends ConsumerProperties, ? extends ProducerProperties>) binder;
			}
		};
		this.bindingServiceProperties = new BindingServiceProperties();
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setContentType("text/plain");
		bindings.put("foo", bindingProperties);
		this.bindingServiceProperties.setBindings(bindings);
		BindingService bindingService = new BindingService(bindingServiceProperties,
				binderFactory);
		MessageConverterConfigurer messageConverterConfigurer = new MessageConverterConfigurer(
				this.bindingServiceProperties,
				new CompositeMessageConverterFactory());
		messageConverterConfigurer.setBeanFactory(Mockito.mock(ConfigurableListableBeanFactory.class));
		messageConverterConfigurer.afterPropertiesSet();
		this.bindingTargetFactory = new SubscribableChannelBindingTargetFactory(messageConverterConfigurer);
		dynamicDestinationsBindable = new DynamicDestinationsBindable();
		this.resolver = new BinderAwareChannelResolver(bindingService, this.bindingTargetFactory,
				dynamicDestinationsBindable);
		this.resolver.setBeanFactory(context.getBeanFactory());
		context.getBeanFactory().registerSingleton("channelResolver", this.resolver);
		context.getBeanFactory().registerSingleton("dynamicDestinationBindable", this.dynamicDestinationsBindable);
		context.registerSingleton("other", DirectChannel.class);
		context.registerSingleton(IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				DefaultMessageBuilderFactory.class);
		context.getBeanFactory().registerSingleton("bindingService", bindingService);
		context.registerSingleton("inputBindingLifecycle", InputBindingLifecycle.class);
		context.registerSingleton("outputBindingLifecycle", OutputBindingLifecycle.class);
		context.refresh();
	}

	@Test
	public void resolveChannel() {
		assertThat(producerBindings).hasSize(0);
		MessageChannel registered = resolver.resolveDestination("foo");
		assertThat(producerBindings).hasSize(1);
		TestBinder.TestBinding binding = producerBindings.get(0);
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
		binder.bindConsumer("foo", null, testChannel, new ConsumerProperties());
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
		context.close();
		assertThat(producerBindings).hasSize(1);
		assertThat(binding.isBound()).isFalse().describedAs("Must not be bound");
	}

	@Test
	public void resolveNonRegisteredChannel() {
		MessageChannel other = resolver.resolveDestination("other");
		assertThat(context.getBean("other")).isSameAs(other);
	}

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void propertyPassthrough() {
		Map<String, BindingProperties> bindings = new HashMap<>();
		BindingProperties genericProperties = new BindingProperties();
		genericProperties.setContentType("text/plain");
		bindings.put("foo", genericProperties);
		this.bindingServiceProperties.setBindings(bindings);
		@SuppressWarnings("unchecked")
		Binder binder = mock(Binder.class);
		Binder binder2 = mock(Binder.class);
		BinderFactory mockBinderFactory = Mockito.mock(BinderFactory.class);
		Binding<MessageChannel> fooBinding = Mockito.mock(Binding.class);
		Binding<MessageChannel> barBinding = Mockito.mock(Binding.class);
		when(binder.bindProducer(
				matches("foo"), any(DirectChannel.class), any(ProducerProperties.class))).thenReturn(fooBinding);
		when(binder2.bindProducer(
				matches("bar"), any(DirectChannel.class), any(ProducerProperties.class))).thenReturn(barBinding);
		when(mockBinderFactory.getBinder(null, DirectChannel.class)).thenReturn(binder);
		when(mockBinderFactory.getBinder("someTransport", DirectChannel.class)).thenReturn(binder2);
		BindingService bindingService = new BindingService(bindingServiceProperties,
				mockBinderFactory);
		@SuppressWarnings("unchecked")
		BinderAwareChannelResolver resolver = new BinderAwareChannelResolver(bindingService, this.bindingTargetFactory,
				new DynamicDestinationsBindable());
		BeanFactory beanFactory = new DefaultListableBeanFactory();
		resolver.setBeanFactory(beanFactory);
		SubscribableChannel resolved = (SubscribableChannel) resolver.resolveDestination("foo");
		verify(binder).bindProducer(eq("foo"), any(MessageChannel.class), any(ProducerProperties.class));
		assertThat(resolved).isSameAs(beanFactory.getBean("foo"));
	}

	/**
	 * A simple test binder that creates queues for the destinations. Ignores groups.
	 */
	private class TestBinder implements Binder<MessageChannel, ConsumerProperties, ProducerProperties> {

		private final Map<String, DirectChannel> destinations = new ConcurrentHashMap<>();

		@Override
		public Binding<MessageChannel> bindConsumer(String name, String group,
				MessageChannel inboundBindTarget, ConsumerProperties properties) {
			synchronized (destinations) {
				if (!destinations.containsKey(name)) {
					destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(inboundBindTarget);
			destinations.get(name).subscribe(directHandler);
			return new TestBinding(name, directHandler);
		}

		@Override
		public Binding<MessageChannel> bindProducer(String name,
				MessageChannel outboundBindTarget, ProducerProperties properties) {
			synchronized (destinations) {
				if (!destinations.containsKey(name)) {
					destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(destinations.get(name));
			// for test purposes we can assume it is a SubscribableChannel
			((SubscribableChannel) outboundBindTarget).subscribe(directHandler);
			TestBinding binding = new TestBinding(name, directHandler);
			producerBindings.add(binding);
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
				bound = false;
				destinations.get(name).unsubscribe(directHandler);
			}

			public boolean isBound() {
				return bound;
			}
		}
	}
}
