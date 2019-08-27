package org.springframework.cloud.stream.binder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.integration.dispatcher.AbstractDispatcher;
import org.springframework.messaging.MessageHandler;

public class BinderErrorChannelTests {

	private static final LastSubscriberMessageHandler FINAL_HANDLER = message -> {

	};

	private static final MessageHandler FIRST_HANDLER = message -> {

	};

	private static final MessageHandler SECOND_HANDLER = message -> {

	};

	@Test
	public void testSubscribeUnsubscribe() {
		BinderErrorChannel channel = new BinderErrorChannel();
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(channel);
		AbstractDispatcher dispatcher = (AbstractDispatcher) fieldAccessor
				.getPropertyValue("dispatcher");

		assertNotNull(dispatcher);
		assertEquals(0, channel.subscribers());
		assertEquals(0, dispatcher.getHandlerCount());

		channel.subscribe(FINAL_HANDLER);

		assertEquals(1, channel.subscribers());
		assertEquals(1, dispatcher.getHandlerCount());

		channel.subscribe(FIRST_HANDLER);

		assertEquals(2, channel.subscribers());
		assertEquals(2, dispatcher.getHandlerCount());

		channel.subscribe(SECOND_HANDLER);

		assertEquals(3, channel.subscribers());
		assertEquals(3, dispatcher.getHandlerCount());

		channel.unsubscribe(FIRST_HANDLER);

		assertEquals(2, channel.subscribers());
		assertEquals(2, dispatcher.getHandlerCount());

		channel.unsubscribe(FINAL_HANDLER);

		assertEquals(1, channel.subscribers());
		assertEquals(1, dispatcher.getHandlerCount());

		channel.unsubscribe(SECOND_HANDLER);

		assertEquals(0, channel.subscribers());
		assertEquals(0, dispatcher.getHandlerCount());
	}

}
