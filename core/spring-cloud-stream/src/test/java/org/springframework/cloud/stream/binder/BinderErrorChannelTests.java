/*
 * Copyright 2017-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.integration.dispatcher.AbstractDispatcher;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Anshul Mehra
 */
public class BinderErrorChannelTests {

	private static final LastSubscriberMessageHandler FINAL_HANDLER = message -> {

	};

	private static final MessageHandler FIRST_HANDLER = message -> {

	};

	private static final MessageHandler SECOND_HANDLER = message -> {

	};

	@Test
	void testExceptionIsThrownWhenNoSubscribers() {
		BinderErrorChannel channel = new BinderErrorChannel();
		Assertions.assertThrows(MessageDeliveryException.class, () -> {
			channel.send(new GenericMessage<String>("hello"));
		});
	}

	@Test
	void testSubscribeUnsubscribe() {
		BinderErrorChannel channel = new BinderErrorChannel();
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(channel);
		AbstractDispatcher dispatcher = (AbstractDispatcher) fieldAccessor
				.getPropertyValue("dispatcher");

		assertThat(dispatcher).isNotNull();
		assertThat(channel.subscribers()).isEqualTo(0);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(0);

		channel.subscribe(FINAL_HANDLER);

		assertThat(channel.subscribers()).isEqualTo(1);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(1);

		channel.subscribe(FIRST_HANDLER);

		assertThat(channel.subscribers()).isEqualTo(2);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(2);

		channel.subscribe(SECOND_HANDLER);

		assertThat(channel.subscribers()).isEqualTo(3);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(3);

		channel.unsubscribe(FIRST_HANDLER);

		assertThat(channel.subscribers()).isEqualTo(2);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(2);

		channel.unsubscribe(FINAL_HANDLER);

		assertThat(channel.subscribers()).isEqualTo(1);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(1);

		channel.unsubscribe(SECOND_HANDLER);

		assertThat(channel.subscribers()).isEqualTo(0);
		assertThat(dispatcher.getHandlerCount()).isEqualTo(0);
	}

}
