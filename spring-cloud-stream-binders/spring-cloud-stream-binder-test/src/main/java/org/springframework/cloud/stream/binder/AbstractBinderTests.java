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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.collection.IsArrayContainingInAnyOrder.arrayContainingInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Mark Fisher
 */
public abstract class AbstractBinderTests {

	protected static final Collection<MediaType> ALL = Collections.singletonList(MediaType.ALL);

	protected AbstractTestBinder<?> testBinder;

	/**
	 * Subclasses may override this default value to have tests wait longer for a message receive, for example if
	 * running
	 * in an environment that is known to be slow (e.g. travis).
	 */
	protected double timeoutMultiplier = 1.0D;

	/**
	 * Attempt to receive a message on the given channel,
	 * waiting up to 1s (times the {@link #timeoutMultiplier}).
	 */
	protected Message<?> receive(PollableChannel channel) {
		return channel.receive((int)(1000 * timeoutMultiplier));
	}

	@Test
	public void testClean() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		Binding<MessageChannel> foo0ProducerBinding = binder.bindProducer("foo.0", new DirectChannel(), null);
		Binding<MessageChannel> foo0ConsumerBinding = binder.bindConsumer("foo.0", "test", new DirectChannel(), null);
		Binding<MessageChannel> foo1ProducerBinding = binder.bindProducer("foo.1", new DirectChannel(), null);
		Binding<MessageChannel> foo1ConsumerBinding = binder.bindConsumer("foo.1", "test", new DirectChannel(), null);
		Binding<MessageChannel> foo2ProducerBinding = binder.bindProducer("foo.2", new DirectChannel(), null);
		Collection<?> bindings = getBindings(binder);
		assertEquals(5, bindings.size());
		foo0ProducerBinding.unbind();
		assertEquals(4, bindings.size());
		foo0ConsumerBinding.unbind();
		foo1ProducerBinding.unbind();
		assertEquals(2, bindings.size());
		foo1ConsumerBinding.unbind();
		foo2ProducerBinding.unbind();
		assertTrue(bindings.isEmpty());
	}

	@Test
	public void testSendAndReceive() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.0", moduleOutputChannel, null);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.0", "test", moduleInputChannel, null);
		Message<?> message = MessageBuilder.withPayload("foo").setHeader(MessageHeaders.CONTENT_TYPE,
				"foo/bar").build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
		assertEquals("foo/bar", inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testSendAndReceiveMultipleTopics() throws Exception {
		Binder<MessageChannel> binder = getBinder();

		DirectChannel moduleOutputChannel1 = new DirectChannel();
		DirectChannel moduleOutputChannel2 = new DirectChannel();

		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding1 = binder.bindProducer("foo.x", moduleOutputChannel1, null);
		Binding<MessageChannel> producerBinding2 = binder.bindProducer("foo.y", moduleOutputChannel2, null);

		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer("foo.x", "test", moduleInputChannel, null);
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer("foo.y", "test", moduleInputChannel, null);

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = MessageBuilder.withPayload(testPayload1.getBytes()).build();
		String testPayload2 = "foo" + UUID.randomUUID().toString();
		Message<?> message2 = MessageBuilder.withPayload(testPayload2.getBytes()).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel1.send(message1);
		moduleOutputChannel2.send(message2);


		Message<?> messages[] = new Message[2];
		messages[0] = receive(moduleInputChannel);
		messages[1] = receive(moduleInputChannel);

		assertNotNull(messages[0]);
		assertNotNull(messages[1]);
		assertThat(messages, arrayContainingInAnyOrder(
				hasProperty("payload", equalTo(testPayload1.getBytes())),
				hasProperty("payload", equalTo(testPayload2.getBytes()))));


		binder.unbind(producerBinding1);
		binder.unbind(consumerBinding1);

		binder.unbind(producerBinding2);
		binder.unbind(consumerBinding2);
	}

	@Test
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		Binding<MessageChannel> producerBinding = binder.bindProducer("bar.0", moduleOutputChannel, null);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bar.0", "test", moduleInputChannel, null);
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo").build();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
		assertNull(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	protected Collection<?> getBindings(Binder<MessageChannel> testBinder) {
		if (testBinder instanceof AbstractTestBinder) {
			return getBindingsFromBinder(((AbstractTestBinder<?>) testBinder).getCoreBinder());
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
		System.clearProperty("SCS_KAFKA_TEST_EMBEDDED");
	}

	/**
	 * If appropriate, let the binder middleware settle down a bit while binding/unbinding actually happens.
	 */
	protected void binderBindUnbindLatency() throws InterruptedException {
		// default none
	}

}
