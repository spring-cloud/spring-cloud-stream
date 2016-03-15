/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.tuple.Tuple;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration({MessageChannelConfigurerTests.TestSink.class})
public class MessageChannelConfigurerTests {

	@Autowired @Bindings(TestSink.class)
	private Sink testSink;

	@Test
	public void testContentTypeConfigurer() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				assertThat(message.getPayload(), instanceOf(Tuple.class));
				assertTrue(((Tuple) message.getPayload()).getFieldNames().get(0).equals("message"));
				assertTrue(((Tuple) message.getPayload()).getValue(0).equals("Hi"));
				latch.countDown();
			}
		};
		testSink.input().subscribe(messageHandler);
		testSink.input().send(MessageBuilder.withPayload("{\"message\":\"Hi\"}").build());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		testSink.input().unsubscribe(messageHandler);
	}

	@Test
	public void testHistoryTrackerConfigurer() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				assertTrue("Message header should have tracking history info",
						message.getHeaders().containsKey("SPRING_CLOUD_STREAM_HISTORY"));
				@SuppressWarnings("unchecked")
				Map<String, String> headerValue = ((Map<String, String>) ((List<?>) message.getHeaders()
						.get("SPRING_CLOUD_STREAM_HISTORY")).get(0));
				String inputBindingProps = headerValue.get("input");
				assertTrue(inputBindingProps.contains("destination=configure"));
				assertTrue(inputBindingProps.contains("trackHistory=true"));
				assertTrue(headerValue.get("instanceIndex").equals("0"));
				assertTrue(headerValue.get("instanceCount").equals("1"));
				latch.countDown();
			}
		};
		testSink.input().subscribe(messageHandler);
		testSink.input().send(MessageBuilder.withPayload("{\"test\":\"value\"}").build());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		testSink.input().unsubscribe(messageHandler);
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/sink-channel-configurers.properties")
	public static class TestSink {

	}
}
