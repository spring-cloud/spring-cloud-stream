/*
 * Copyright 2020-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.io.IOException;
import java.util.Collections;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ErrorMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 3.0.6
 *
 */
public class RepublishUnitTests {

	@Test
	public void testBadRepublishSetting() throws IOException {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection conn = mock(Connection.class);
		given(cf.createConnection()).willReturn(conn);
		Channel channel = mock(Channel.class);
		given(channel.isOpen()).willReturn(true);
		given(channel.exchangeDeclarePassive("DLX")).willThrow(new IOException());
		given(conn.createChannel(false)).willReturn(channel);
		RabbitProperties props = new RabbitProperties();
		RabbitMessageChannelBinder binder = new RabbitMessageChannelBinder(cf, props, null);
		RabbitConsumerProperties extension = new RabbitConsumerProperties();
		ExtendedConsumerProperties<RabbitConsumerProperties> bindingProps =
				new ExtendedConsumerProperties<>(extension);
		MessageHandler handler = binder.getErrorMessageHandler(mock(ConsumerDestination.class), "foo", bindingProps);
		ErrorMessage message = new ErrorMessage(new RuntimeException("test"),
				Collections.singletonMap(IntegrationMessageHeaderAccessor.SOURCE_DATA,
						new Message("foo".getBytes(), new MessageProperties())));
		handler.handleMessage(message);
		handler.handleMessage(message);
		verify(channel, times(1)).exchangeDeclarePassive("DLX");
		verify(channel, never()).basicPublish(any(), any(), eq(false), any(), any());
	}

}
