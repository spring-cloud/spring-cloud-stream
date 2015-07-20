/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.cloud.stream.binder.local;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.cloud.stream.binder.AbstractBinderTests;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.cloud.stream.binder.Binder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Gary Russell
 * @author David Turanski
 * @since 1.0
 */
public class LocalBinderTests extends AbstractBinderTests {

	@Override
	protected Binder getBinder() throws Exception {
		LocalMessageChannelBinder binder = new LocalMessageChannelBinder();
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton(
				IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				new DefaultMessageBuilderFactory());
		applicationContext.refresh();
		binder.setApplicationContext(applicationContext);
		binder.setExecutorCorePoolSize(2);
		binder.setExecutorMaxPoolSize(10);
		binder.setExecutorKeepAliveSeconds(59);
		binder.setExecutorQueueSize(Integer.MAX_VALUE - 1);
		binder.afterPropertiesSet();
		return binder;
	}

	protected Collection<?> getBindings(Binder testBinder) {
		return getBindingsFromBinder(testBinder);
	}

	@Test
	public void testProps() throws Exception {
		LocalMessageChannelBinder binder = (LocalMessageChannelBinder) getBinder();
		ThreadPoolTaskExecutor exec = TestUtils.getPropertyValue(binder, "executor", ThreadPoolTaskExecutor.class);
		assertEquals(2, exec.getCorePoolSize());
		assertEquals(10, exec.getMaxPoolSize());
		assertEquals(59, exec.getKeepAliveSeconds());
		Assert.assertEquals(Integer.MAX_VALUE - 1, TestUtils.getPropertyValue(exec, "queueCapacity"));
	}

	@Test
	public void testPayloadConversionNotNeededExplicitType() throws Exception {
		LocalMessageChannelBinder binder = (LocalMessageChannelBinder) getBinder();
		verifyPayloadConversion(new TestPayload(), binder);
	}

	@Test
	public void testNoPayloadConversionByDefault() throws Exception {
		LocalMessageChannelBinder binder = (LocalMessageChannelBinder) getBinder();
		verifyPayloadConversion(new TestPayload(), binder);
	}

	@Test
	public void testTapDoesntHurtStream() throws Exception {
		LocalMessageChannelBinder binder = (LocalMessageChannelBinder) getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		moduleOutputChannel.setBeanName("bangOut");
		DirectChannel tapChannel = new DirectChannel();
		tapChannel.setBeanName("tapChannel");
		WireTap tap = new WireTap(tapChannel);
		moduleOutputChannel.addInterceptor(tap);
		binder.bindProducer("bang.0", moduleOutputChannel, null);
		final AtomicBoolean messageReceived = new AtomicBoolean();
		final AtomicReference<Thread> streamThread = new AtomicReference<Thread>();
		binder.bindConsumer("bang.0", new DirectChannel() {

			@Override
			protected boolean doSend(Message<?> message, long timeout) {
				messageReceived.set(true);
				streamThread.set(Thread.currentThread());
				return true;
			}
		}, null);
		final CountDownLatch tapped = new CountDownLatch(1);
		final AtomicReference<Thread> tapThread = new AtomicReference<Thread>();
		binder.bindPubSubProducer("tap:stream:bang.0", tapChannel, null);
		binder.bindPubSubConsumer("tap:stream:bang.0", new DirectChannel() {

			@Override
			protected boolean doSend(Message<?> message, long timeout) {
				tapThread.set(Thread.currentThread());
				tapped.countDown();
				throw new RuntimeException("bang");
			}
		}, null);
		moduleOutputChannel.send(new GenericMessage<String>("Foo"));
		assertTrue(tapped.await(10, TimeUnit.SECONDS));
		assertTrue(messageReceived.get());
		assertSame(Thread.currentThread(), streamThread.get());
		assertNotNull(tapThread.get());
		assertNotSame(Thread.currentThread(), tapThread.get());
	}

	private void verifyPayloadConversion(final Object expectedValue, final LocalMessageChannelBinder binder) {
		DirectChannel myChannel = new DirectChannel();
		binder.bindConsumer("in", myChannel, null);
		DirectChannel input = binder.getBean("in", DirectChannel.class);
		assertNotNull(input);

		final AtomicBoolean msgSent = new AtomicBoolean(false);

		myChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				assertEquals(expectedValue, message.getPayload());
				msgSent.set(true);
			}
		});

		Message<TestPayload> msg = MessageBuilder.withPayload(new TestPayload())
				.setHeader(MessageHeaders.CONTENT_TYPE, MediaType.ALL_VALUE).build();

		input.send(msg);
		assertTrue(msgSent.get());
	}

	static class TestPayload {

		@Override
		public String toString() {
			return "foo";
		}

		@Override
		public boolean equals(Object other) {
			return (other instanceof TestPayload && this.toString().equals(other.toString()));
		}

		@Override
		public int hashCode() {
			return this.toString().hashCode();
		}

	}
}
