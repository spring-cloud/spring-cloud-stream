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

package org.springframework.xd.dirt.integration.bus;

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
import org.springframework.integration.channel.interceptor.WireTap;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.xd.dirt.integration.bus.MessageBus.Capability;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;
import org.springframework.xd.dirt.integration.bus.serializer.kryo.PojoCodec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 */
public abstract class AbstractMessageBusTests {

	protected static final Collection<MediaType> ALL = Collections.singletonList(MediaType.ALL);

	protected AbstractTestMessageBus<?> testMessageBus;

	@Test
	public void testClean() throws Exception {
		MessageBus messageBus = getMessageBus();
		messageBus.bindProducer("foo.0", new DirectChannel(), null);
		messageBus.bindConsumer("foo.0", new DirectChannel(), null);
		messageBus.bindProducer("foo.1", new DirectChannel(), null);
		messageBus.bindConsumer("foo.1", new DirectChannel(), null);
		messageBus.bindProducer("foo.2", new DirectChannel(), null);
		Collection<?> bindings = getBindings(messageBus);
		assertEquals(5, bindings.size());
		messageBus.unbindProducers("foo.0");
		assertEquals(4, bindings.size());
		messageBus.unbindConsumers("foo.0");
		messageBus.unbindProducers("foo.1");
		assertEquals(2, bindings.size());
		messageBus.unbindConsumers("foo.1");
		messageBus.unbindProducers("foo.2");
		assertTrue(bindings.isEmpty());
	}

	@Test
	public void testSendAndReceive() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		messageBus.bindProducer("foo.0", moduleOutputChannel, null);
		messageBus.bindConsumer("foo.0", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		// Let the consumer actually bind to the producer before sending a msg
		busBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
		assertEquals("foo/bar", inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		messageBus.unbindProducers("foo.0");
		messageBus.unbindConsumers("foo.0");
	}

	@Test
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		messageBus.bindProducer("bar.0", moduleOutputChannel, null);
		messageBus.bindConsumer("bar.0", moduleInputChannel, null);
		busBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo").build();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
		assertNull(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		messageBus.unbindProducers("bar.0");
		messageBus.unbindConsumers("bar.0");
	}

	@Test
	public void testSendAndReceivePubSub() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		DirectChannel tapChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		messageBus.bindProducer("baz.0", moduleOutputChannel, null);
		messageBus.bindConsumer("baz.0", moduleInputChannel, null);
		moduleOutputChannel.addInterceptor(new WireTap(tapChannel));
		messageBus.bindPubSubProducer("tap:baz.http", tapChannel, null);
		// A new module is using the tap as an input channel
		String fooTapName = messageBus.isCapable(Capability.DURABLE_PUBSUB) ? "foo.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(fooTapName, module2InputChannel, null);
		// Another new module is using tap as an input channel
		String barTapName = messageBus.isCapable(Capability.DURABLE_PUBSUB) ? "bar.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(barTapName, module3InputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(5000);
			assertNotNull(inbound);
			assertEquals("foo", inbound.getPayload());
			assertNull(inbound.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
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
			assertNull(tapped1.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped1.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			assertEquals("foo", tapped2.getPayload());
			assertNull(tapped2.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped2.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		}
		// delete one tap stream is deleted
		messageBus.unbindConsumer(barTapName, module3InputChannel);
		Message<?> message2 = MessageBuilder.withPayload("bar").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		messageBus.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		messageBus.unbindConsumer("baz.0", moduleInputChannel);
		messageBus.unbindProducer("baz.0", moduleOutputChannel);
		messageBus.unbindProducers("tap:baz.http");
		assertTrue(getBindings(messageBus).isEmpty());
	}

	@Test
	public void createInboundPubSubBeforeOutboundPubSub() throws Exception {
		MessageBus messageBus = getMessageBus();
		DirectChannel moduleOutputChannel = new DirectChannel();
		// Test pub/sub by emulating how StreamPlugin handles taps
		DirectChannel tapChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		QueueChannel module2InputChannel = new QueueChannel();
		QueueChannel module3InputChannel = new QueueChannel();
		// Create the tap first
		String fooTapName = messageBus.isCapable(Capability.DURABLE_PUBSUB) ? "foo.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(fooTapName, module2InputChannel, null);

		// Then create the stream
		messageBus.bindProducer("baz.0", moduleOutputChannel, null);
		messageBus.bindConsumer("baz.0", moduleInputChannel, null);
		moduleOutputChannel.addInterceptor(new WireTap(tapChannel));
		messageBus.bindPubSubProducer("tap:baz.http", tapChannel, null);

		// Another new module is using tap as an input channel
		String barTapName = messageBus.isCapable(Capability.DURABLE_PUBSUB) ? "bar.tap:baz.http" : "tap:baz.http";
		messageBus.bindPubSubConsumer(barTapName, module3InputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		boolean success = false;
		boolean retried = false;
		while (!success) {
			moduleOutputChannel.send(message);
			Message<?> inbound = moduleInputChannel.receive(5000);
			assertNotNull(inbound);
			assertEquals("foo", inbound.getPayload());
			assertNull(inbound.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
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
			assertNull(tapped1.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped1.getHeaders().get(MessageHeaders.CONTENT_TYPE));
			assertEquals("foo", tapped2.getPayload());
			assertNull(tapped2.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
			assertEquals("foo/bar", tapped2.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		}
		// delete one tap stream is deleted
		messageBus.unbindConsumer(barTapName, module3InputChannel);
		Message<?> message2 = MessageBuilder.withPayload("bar").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		moduleOutputChannel.send(message2);

		// other tap still receives messages
		Message<?> tapped = module2InputChannel.receive(5000);
		assertNotNull(tapped);

		// Removed tap does not
		assertNull(module3InputChannel.receive(1000));

		// when other tap stream is deleted
		messageBus.unbindConsumer(fooTapName, module2InputChannel);
		// Clean up as StreamPlugin would
		messageBus.unbindConsumer("baz.0", moduleInputChannel);
		messageBus.unbindProducer("baz.0", moduleOutputChannel);
		messageBus.unbindProducers("tap:baz.http");
		assertTrue(getBindings(messageBus).isEmpty());
	}

	@Test
	public void testBadDynamic() throws Exception {
		Properties properties = new Properties();
		properties.setProperty(BusProperties.PARTITION_KEY_EXPRESSION, "'foo'");
		MessageBus messageBus = getMessageBus();
		try {
			messageBus.bindDynamicProducer("queue:foo", properties);
			fail("Exception expected");
		}
		catch (MessageBusException mbe) {
			Assert.assertEquals("Failed to bind dynamic channel 'queue:foo' with properties " +
							"{partitionKeyExpression='foo'}",
					mbe.getMessage());
			if (messageBus instanceof AbstractTestMessageBus) {
				messageBus = ((AbstractTestMessageBus) messageBus).getCoreMessageBus();
			}
			assertFalse(((MessageBusSupport) messageBus).getApplicationContext().containsBean("queue:foo"));
		}
	}

	protected Collection<?> getBindings(MessageBus testMessageBus) {
		if (testMessageBus instanceof AbstractTestMessageBus) {
			return getBindingsFromMsgBus(((AbstractTestMessageBus) testMessageBus).getCoreMessageBus());
		}
		return Collections.EMPTY_LIST;
	}

	protected Collection<?> getBindingsFromMsgBus(MessageBus messageBus) {
		DirectFieldAccessor accessor = new DirectFieldAccessor(messageBus);
		return (List<?>) accessor.getPropertyValue("bindings");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	protected MultiTypeCodec<Object> getCodec() {
		return new PojoCodec();
	}

	protected abstract MessageBus getMessageBus() throws Exception;

	@After
	public void cleanup() {
		if (testMessageBus != null) {
			testMessageBus.cleanup();
		}
	}

	/**
	 * If appropriate, let the bus middleware settle down a bit while binding/unbinding actually happens.
	 */
	protected void busBindUnbindLatency() throws InterruptedException {
		// default none
	}

}
