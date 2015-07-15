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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.xd.dirt.integration.bus.local.LocalMessageBus;

/**
 * @author Mark Fisher
 * @author Gary Russell
 */
public class MessageBusAwareChannelResolverTests {

	private final StaticApplicationContext context = new StaticApplicationContext();

	private volatile MessageBusAwareChannelResolver resolver;

	private volatile LocalMessageBus bus;

	@Before
	public void setupContext() throws Exception {
		this.bus = new LocalMessageBus();
		this.bus.setApplicationContext(context);
		this.bus.afterPropertiesSet();
		this.resolver = new MessageBusAwareChannelResolver(this.bus, null);
		this.resolver.setBeanFactory(context);
		context.getBeanFactory().registerSingleton("channelResolver",
				this.resolver);
		context.registerSingleton("other", DirectChannel.class);
		context.registerSingleton("taskScheduler", ThreadPoolTaskScheduler.class);
		context.registerSingleton(IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				DefaultMessageBuilderFactory.class);
		context.refresh();

		PollerMetadata poller = new PollerMetadata();
		poller.setTrigger(new PeriodicTrigger(1000));
		bus.setPoller(poller);
	}

	@Test
	public void resolveQueueChannel() {
		MessageChannel registered = resolver.resolveDestination("queue:foo");
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
		bus.bindConsumer("queue:foo", testChannel, null);
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
	public void resolveTopicChannel() {
		MessageChannel registered = resolver.resolveDestination("topic:bar");
		PublishSubscribeChannel[] testChannels = {
			new PublishSubscribeChannel(), new PublishSubscribeChannel(), new PublishSubscribeChannel()
		};
		final CountDownLatch latch = new CountDownLatch(testChannels.length);
		final List<Message<?>> received = new ArrayList<Message<?>>();
		for (PublishSubscribeChannel testChannel : testChannels) {
			testChannel.subscribe(new MessageHandler() {

				@Override
				public void handleMessage(Message<?> message) throws MessagingException {
					received.add(message);
					latch.countDown();
				}
			});
			bus.bindPubSubConsumer("topic:bar", testChannel, null);
		}
		assertEquals(0, received.size());
		registered.send(MessageBuilder.withPayload("hello").build());
		try {
			assertTrue("latch timed out", latch.await(1, TimeUnit.SECONDS));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			fail("interrupted while awaiting latch");
		}
		assertEquals(3, received.size());
		assertEquals("hello", received.get(0).getPayload());
		assertEquals("hello", received.get(1).getPayload());
		assertEquals("hello", received.get(2).getPayload());
		context.close();
	}

	@Test
	public void resolveNonRegisteredChannel() {
		MessageChannel other = resolver.resolveDestination("other");
		assertSame(context.getBean("other"), other);
	}

	@Test
	public void propertyPassthrough() {
		Properties properties = new Properties();
		MessageBus bus = mock(MessageBus.class);
		doReturn(new DirectChannel()).when(bus).bindDynamicProducer("queue:foo", properties);
		doReturn(new DirectChannel()).when(bus).bindDynamicPubSubProducer("topic:bar", properties);
		MessageBusAwareChannelResolver resolver = new MessageBusAwareChannelResolver(bus, properties);
		BeanFactory beanFactory = new DefaultListableBeanFactory();
		resolver.setBeanFactory(beanFactory);
		resolver.resolveDestination("queue:foo");
		resolver.resolveDestination("topic:bar");
		verify(bus).bindDynamicProducer("queue:foo", properties);
		verify(bus).bindDynamicPubSubProducer("topic:bar", properties);
	}

}
