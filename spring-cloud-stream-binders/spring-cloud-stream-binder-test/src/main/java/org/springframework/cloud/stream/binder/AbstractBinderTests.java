/*
 * Copyright 2013-2014 the original author or authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 */
public abstract class AbstractBinderTests {

	protected static final Collection<MediaType> ALL = Collections.singletonList(MediaType.ALL);

	protected AbstractTestBinder<?> testBinder;

	@Test
	public void testClean() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		binder.bindProducer("foo.0", new DirectChannel(), null);
		binder.bindConsumer("foo.0", new DirectChannel(), null);
		binder.bindProducer("foo.1", new DirectChannel(), null);
		binder.bindConsumer("foo.1", new DirectChannel(), null);
		binder.bindProducer("foo.2", new DirectChannel(), null);
		Collection<?> bindings = getBindings(binder);
		assertEquals(5, bindings.size());
		binder.unbindProducers("foo.0");
		assertEquals(4, bindings.size());
		binder.unbindConsumers("foo.0");
		binder.unbindProducers("foo.1");
		assertEquals(2, bindings.size());
		binder.unbindConsumers("foo.1");
		binder.unbindProducers("foo.2");
		assertTrue(bindings.isEmpty());
	}

	@Test
	public void testSendAndReceive() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		binder.bindProducer("foo.0", moduleOutputChannel, null);
		binder.bindConsumer("foo.0", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
		assertEquals("foo/bar", inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		binder.unbindProducers("foo.0");
		binder.unbindConsumers("foo.0");
	}

	@Test
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		binder.bindProducer("bar.0", moduleOutputChannel, null);
		binder.bindConsumer("bar.0", moduleInputChannel, null);
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo").build();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
		assertNull(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		binder.unbindProducers("bar.0");
		binder.unbindConsumers("bar.0");
	}

	@Test
	public void testSendAndReceivePubSub() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		binder.bindProducer("baz.0", moduleOutputChannel, null);
		binder.bindConsumer("baz.0", moduleInputChannel, null);
		// A new module is using the tap as an input channel
		String fooTapName = "baz.0";
		binder.bindPubSubConsumer(fooTapName, module2InputChannel, "tgroup1", null);
		// Another new module is using tap as an input channel
		String barTapName = "baz.0";
		binder.bindPubSubConsumer(barTapName, module3InputChannel, "tgroup2", null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(5000);
			assertNotNull(inbound);
			assertEquals("foo", inbound.getPayload());
			assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			Message<?> tapped1 = module2InputChannel.receive(5000);
			Message<?> tapped2 = module3InputChannel.receive(5000);
			if (tapped1 == null || tapped2 == null) {
				// listener may not have started
				assertFalse("Failed to receive tap after retry", retried);
				retried = true;
				continue;
			}
			success = true;
			assertEquals("foo", tapped1.getPayload());
			assertNull(tapped1.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped1.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			assertEquals("foo", tapped2.getPayload());
			assertNull(tapped2.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped2.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		}
		// delete one tap stream is deleted
		binder.unbindPubSubConsumers(barTapName, "tgroup2");
		Message<?> message2 = MessageBuilder.withPayload("bar").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		binder.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		binder.unbindConsumer("baz.0", moduleInputChannel);
		binder.unbindProducer("baz.0", moduleOutputChannel);
		binder.unbindPubSubConsumers(fooTapName, "tgroup1");
		assertTrue(getBindings(binder).isEmpty());
	}

	@Test
	public void createInboundPubSubBeforeOutboundPubSub() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		// Create the tap first
		String fooTapName = "baz.0";
		binder.bindPubSubConsumer(fooTapName, module2InputChannel, "tgroup1", null);

		// Then create the stream
		binder.bindProducer("baz.0", moduleOutputChannel, null);
		binder.bindConsumer("baz.0", moduleInputChannel, null);

		// Another new module is using tap as an input channel
		String barTapName = "baz.0";
		binder.bindPubSubConsumer(barTapName, module3InputChannel, "tgroup2", null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(5000);
			assertNotNull(inbound);
			assertEquals("foo", inbound.getPayload());
			assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			Message<?> tapped1 = module2InputChannel.receive(5000);
			Message<?> tapped2 = module3InputChannel.receive(5000);
			if (tapped1 == null || tapped2 == null) {
				// listener may not have started
				assertFalse("Failed to receive tap after retry", retried);
				retried = true;
				continue;
			}
			success = true;
			assertEquals("foo", tapped1.getPayload());
			assertNull(tapped1.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped1.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			assertEquals("foo", tapped2.getPayload());
			assertNull(tapped2.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped2.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		}
		// delete one tap stream is deleted
		binder.unbindPubSubConsumers(barTapName, "tgroup2");
		Message<?> message2 = MessageBuilder.withPayload("bar").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		binder.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		binder.unbindConsumer("baz.0", moduleInputChannel);
		binder.unbindProducer("baz.0", moduleOutputChannel);
		binder.unbindPubSubConsumers(fooTapName, "tgroup1");
		assertTrue(getBindings(binder).isEmpty());
	}

	@Test
	public void testBadDynamic() throws Exception {
		Properties properties = new Properties();
		properties.setProperty(BinderPropertyKeys.PARTITION_KEY_EXPRESSION, "'foo'");
		Binder<MessageChannel> binder = getBinder();
		try {
			binder.bindDynamicProducer("queue:foo", properties);
			fail("Exception expected");
		}
		catch (BinderException mbe) {
			Assert.assertEquals("Failed to bind dynamic channel 'queue:foo' with properties " +
							"{partitionKeyExpression='foo'}",
					mbe.getMessage());
			if (binder instanceof AbstractTestBinder) {
				binder = ((AbstractTestBinder) binder).getCoreBinder();
			}
			assertFalse(((MessageChannelBinderSupport) binder).getApplicationContext().containsBean("queue:foo"));
		}
	}

	protected Collection<?> getBindings(Binder<MessageChannel> testBinder) {
		if (testBinder instanceof AbstractTestBinder) {
			return getBindingsFromBinder(((AbstractTestBinder) testBinder).getCoreBinder());
		}
		return Collections.EMPTY_LIST;
	}

	protected Collection<?> getBindingsFromBinder(Binder<MessageChannel> binder) {
		DirectFieldAccessor accessor = new DirectFieldAccessor(binder);
		return (List<?>) accessor.getPropertyValue("bindings");
	}

	protected abstract Binder<MessageChannel> getBinder() throws Exception;

	@After
	public void cleanup() {
		if (testBinder != null) {
			testBinder.cleanup();
		}
	}

	/**
	 * If appropriate, let the binder middleware settle down a bit while binding/unbinding actually happens.
	 */
	protected void binderBindUnbindLatency() throws InterruptedException {
		// default none
	}

}
