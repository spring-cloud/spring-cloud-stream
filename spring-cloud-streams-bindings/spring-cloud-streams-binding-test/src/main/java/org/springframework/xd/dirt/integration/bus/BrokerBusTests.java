/*
 * Copyright 2014 the original author or authors.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;


/**
 * Tests for buses that use an external broker.
 *
 * @author Gary Russell
 */
public abstract class BrokerBusTests extends
AbstractMessageBusTests {

	@Test
	public void testDirectBinding() throws Exception {
		MessageBus bus = getMessageBus();
		Properties properties = new Properties();
		properties.setProperty(BusProperties.DIRECT_BINDING_ALLOWED, "true");

		DirectChannel moduleInputChannel = new DirectChannel();
		moduleInputChannel.setBeanName("direct.input");
		DirectChannel moduleOutputChannel = new DirectChannel();
		moduleOutputChannel.setBeanName("direct.output");
		bus.bindConsumer("direct.0", moduleInputChannel, null);
		bus.bindProducer("direct.0", moduleOutputChannel, properties);

		final AtomicReference<Thread> caller = new AtomicReference<Thread>();
		final AtomicInteger count = new AtomicInteger();
		moduleInputChannel.subscribe(new MessageHandler() {

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				caller.set(Thread.currentThread());
				count.incrementAndGet();
			}
		});

		moduleOutputChannel.send(new GenericMessage<String>("foo"));
		moduleOutputChannel.send(new GenericMessage<String>("foo"));

		assertNotNull(caller.get());
		assertSame(Thread.currentThread(), caller.get());
		assertEquals(2, count.get());
		assertNull(spyOn("direct.0").receive(true));

		// Remove direct binding and bind producer to the bus
		bus.unbindConsumers("direct.0");
		busBindUnbindLatency();

		Spy spy = spyOn("direct.0");
		count.set(0);
		moduleOutputChannel.send(new GenericMessage<String>("bar"));
		moduleOutputChannel.send(new GenericMessage<String>("baz"));
		Object bar = spy.receive(false);
		assertEquals("bar", bar);
		Object baz = spy.receive(false);
		assertEquals("baz", baz);
		assertEquals(0, count.get());

		// Unbind producer from bus and bind directly again
		caller.set(null);
		bus.bindConsumer("direct.0", moduleInputChannel, null);
		moduleOutputChannel.send(new GenericMessage<String>("foo"));
		moduleOutputChannel.send(new GenericMessage<String>("foo"));
		assertNotNull(caller.get());
		assertSame(Thread.currentThread(), caller.get());
		assertEquals(2, count.get());
		assertNull(spy.receive(true));

		bus.unbindProducers("direct.0");
		bus.unbindConsumers("direct.0");
	}

	/**
	 * Create a new spy on the given 'queue'. This allows de-correlating the creation of
	 * the 'connection' from its actual usage, which may be needed by some implementations to
	 * see messages sent after connection creation.
	 */
	public abstract Spy spyOn(final String name);



}
