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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.DynamicBindable;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.ChannelBindingServiceProperties;
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

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public class BinderAwareChannelResolverTests {

	private final StaticApplicationContext context = new StaticApplicationContext();

	private volatile BinderAwareChannelResolver resolver;

	private volatile Binder<MessageChannel> binder;

	@Before
	public void setupContext() throws Exception {
		this.binder = new TestBinder();
		BinderFactory binderFactory = new BinderFactory<MessageChannel>() {
			@Override
			public Binder<MessageChannel> getBinder(String configurationName) {
				return binder;
			}
		};
		this.resolver = new BinderAwareChannelResolver(binderFactory, null, new DynamicBindable());
		this.resolver.setBeanFactory(context.getBeanFactory());
		context.getBeanFactory().registerSingleton("channelResolver",
				this.resolver);
		context.registerSingleton("other", DirectChannel.class);
		context.registerSingleton(IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				DefaultMessageBuilderFactory.class);
		context.refresh();
	}

	@Test
	public void resolveChannel() {
		MessageChannel registered = resolver.resolveDestination("foo");
		DirectChannel testChannel = new DirectChannel();
		final CountDownLatch latch = new CountDownLatch(1);
		final List<Message<?>> received = new ArrayList<Message<?>>();
		testChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				received.add(message);
				latch.countDown();
			}
		});
		binder.bindConsumer("foo", null, testChannel, null);
		assertEquals(0, received.size());
		registered.send(MessageBuilder.withPayload("hello").build());
		try {
			assertTrue("latch timed out", latch.await(1, TimeUnit.SECONDS));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			fail("interrupted while awaiting latch");
		}
		assertEquals(1, received.size());
		assertEquals("hello", received.get(0).getPayload());
		context.close();
	}

	@Test
	public void resolveNonRegisteredChannel() {
		MessageChannel other = resolver.resolveDestination("other");
		assertSame(context.getBean("other"), other);
	}

	@Test
	@SuppressWarnings("rawtypes")
	public void propertyPassthrough() {
		ChannelBindingServiceProperties bindingServiceProperties = new ChannelBindingServiceProperties();
		DynamicBindable dynamicBindable = new DynamicBindable();
		Map<String, BindingProperties> bindings = new HashMap<String, BindingProperties>();
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setContentType("text/plain");
		bindings.put("foo", bindingProperties);
		bindingServiceProperties.setBindings(bindings);
		@SuppressWarnings("unchecked")
		Binder<MessageChannel> binder = mock(Binder.class);
		Binder<MessageChannel> binder2 = mock(Binder.class);
		BinderFactory mockBinderFactory = Mockito.mock(BinderFactory.class);
		Binding<MessageChannel> fooBinding = Mockito.mock(Binding.class);
		Binding<MessageChannel> barBinding = Mockito.mock(Binding.class);
		when(binder.bindProducer(
				matches("foo"), any(DirectChannel.class), any(Properties.class))).thenReturn(fooBinding);
		when(binder2.bindProducer(
				matches("bar"), any(DirectChannel.class), any(Properties.class))).thenReturn(barBinding);
		when(mockBinderFactory.getBinder(null)).thenReturn(binder);
		when(mockBinderFactory.getBinder("someTransport")).thenReturn(binder2);
		@SuppressWarnings("unchecked")
		BinderAwareChannelResolver resolver =
				new BinderAwareChannelResolver(mockBinderFactory, bindingServiceProperties, dynamicBindable);
		BeanFactory beanFactory = new DefaultListableBeanFactory();
		resolver.setBeanFactory(beanFactory);
		MessageChannel resolved = resolver.resolveDestination("foo");
		verify(binder).bindProducer(eq("foo"), any(MessageChannel.class), any(Properties.class));
		assertSame(resolved, beanFactory.getBean("foo"));
		resolved = resolver.resolveDestination("someTransport:bar");
		verify(binder2).bindProducer(eq("bar"), any(MessageChannel.class), any(Properties.class));
		assertSame(resolved, beanFactory.getBean("someTransport:bar"));
		assertTrue("Dynamic bindable should have two destination names", dynamicBindable.getOutputs().size() == 2);
		assertTrue("Dynamic bindable should have the destination name 'foo'", dynamicBindable.getOutputs().contains("foo"));
		assertTrue("Dynamic bindable should have the destination name 'bar'", dynamicBindable.getOutputs().contains("someTransport:bar"));
	}

	/**
	 * A simple test binder that creates queues for the destinations. Ignores groups.
	 */
	private class TestBinder implements Binder<MessageChannel> {

		private final Map<String, DirectChannel> destinations = new ConcurrentHashMap<>();

		@Override
		public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel inboundBindTarget,
				Properties properties) {
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
		public Binding<MessageChannel> bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
			synchronized (destinations) {
				if (!destinations.containsKey(name)) {
					destinations.put(name, new DirectChannel());
				}
			}
			DirectHandler directHandler = new DirectHandler(destinations.get(name));
			// for test purposes we can assume it is a SubscribableChannel
			((SubscribableChannel) outboundBindTarget).subscribe(directHandler);
			return new TestBinding(name, directHandler);
		}

		private class TestBinding implements Binding<MessageChannel> {

			private final String name;

			private final DirectHandler directHandler;

			private TestBinding(String name, DirectHandler directHandler) {
				this.name = name;
				this.directHandler = directHandler;
			}

			@Override
			public void unbind() {
				destinations.get(name).unsubscribe(directHandler);
			}
		}
	}
}
