/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = ErrorChannelTests.TestSource.class)
public class ErrorChannelTests {

	@Autowired
	@Qualifier(BindingServiceConfiguration.ERROR_BRIDGE_CHANNEL)
	private MessageChannel errorBridgeChannel;

	@Autowired
	private BinderFactory binderFactory;

	@Test
	public void testErrorChannelBinding() throws Exception {
		Message<?> message = ((TestSupportBinder) binderFactory.getBinder(null, MessageChannel.class))
				.messageCollector().forChannel(errorBridgeChannel).poll(10, TimeUnit.SECONDS);
		Assert.isTrue(message instanceof ErrorMessage, "Message should be an instance of ErrorMessage");
		Assert.isTrue(message.getPayload() instanceof MessagingException, "Message payload should be an instance" +
				"of MessagingException");
		Assert.isTrue(message.getPayload().toString()
				.equals("org.springframework.messaging.MessagingException: test"), "Text did not match");
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/errorchannel/source-channel.properties")
	public static class TestSource {

		@Bean
		@InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "5000", maxMessagesPerPoll = "1"))
		public MessageSource<String> timerMessageSource() {
			return () -> {
				throw new MessagingException("test");
			};
		}
	}
}
