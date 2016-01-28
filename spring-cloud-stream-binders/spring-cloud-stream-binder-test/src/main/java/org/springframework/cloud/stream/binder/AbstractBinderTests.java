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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
		binder.bindConsumer("foo.0", "test", new DirectChannel(), null);
		binder.bindProducer("foo.1", new DirectChannel(), null);
		binder.bindConsumer("foo.1", "test", new DirectChannel(), null);
		binder.bindProducer("foo.2", new DirectChannel(), null);
		Collection<?> bindings = getBindings(binder);
		assertEquals(5, bindings.size());
		binder.unbindProducers("foo.0");
		assertEquals(4, bindings.size());
		binder.unbindConsumers("foo.0", "test");
		binder.unbindProducers("foo.1");
		assertEquals(2, bindings.size());
		binder.unbindConsumers("foo.1", "test");
		binder.unbindProducers("foo.2");
		assertTrue(bindings.isEmpty());
	}

	@Test
	public void testSendAndReceive() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		binder.bindProducer("foo.0", moduleOutputChannel, null);
		binder.bindConsumer("foo.0", "test", moduleInputChannel, null);
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
		binder.unbindConsumers("foo.0", "test");
	}

	@Test
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		Binder<MessageChannel> binder = getBinder();
		DirectChannel moduleOutputChannel = new DirectChannel();
		QueueChannel moduleInputChannel = new QueueChannel();
		binder.bindProducer("bar.0", moduleOutputChannel, null);
		binder.bindConsumer("bar.0", "test", moduleInputChannel, null);
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo").build();
		moduleOutputChannel.send(message);
		Message<?> inbound = moduleInputChannel.receive(5000);
		assertNotNull(inbound);
		assertEquals("foo", inbound.getPayload());
		assertNull(inbound.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
		assertNull(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		binder.unbindProducers("bar.0");
		binder.unbindConsumers("bar.0", "test");
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
	}

	/**
	 * If appropriate, let the binder middleware settle down a bit while binding/unbinding actually happens.
	 */
	protected void binderBindUnbindLatency() throws InterruptedException {
		// default none
	}

}
