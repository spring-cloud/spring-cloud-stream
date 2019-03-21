/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.tuple.Tuple;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MessageChannelConfigurerTests.TestSink.class})
public class MessageChannelConfigurerTests {

	@Autowired
	@Bindings(TestSink.class)
	private Sink testSink;

	@Autowired
	private CompositeMessageConverterFactory messageConverterFactory;

	@Autowired
	private ObjectMapper objectMapper;

	@Test
	public void testMessageConverterConfigurer() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				assertThat(message.getPayload()).isInstanceOf(Tuple.class);
				assertThat(((Tuple) message.getPayload()).getFieldNames().get(0)).isEqualTo("message");
				assertThat(((Tuple) message.getPayload()).getValue(0)).isEqualTo("Hi");
				latch.countDown();
			}
		};
		testSink.input().subscribe(messageHandler);
		testSink.input().send(MessageBuilder.withPayload("{\"message\":\"Hi\"}").build());
		assertThat(latch.await(10, TimeUnit.SECONDS));
		testSink.input().unsubscribe(messageHandler);
	}

	@Test
	public void testObjectMapperConfig() throws Exception {
		CompositeMessageConverter converters = (CompositeMessageConverter) messageConverterFactory.getMessageConverterForType(MimeTypeUtils
				.APPLICATION_JSON);
		for (MessageConverter converter : converters.getConverters()) {
			DirectFieldAccessor converterAccessor = new DirectFieldAccessor(converter);
			ObjectMapper objectMapper = (ObjectMapper) converterAccessor.getPropertyValue("objectMapper");
			// assert that the ObjectMapper used by the converters is compliant with the Boot configuration
			assertThat(!objectMapper.getSerializationConfig().isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS))
					.withFailMessage("SerializationFeature 'WRITE_DATES_AS_TIMESTAMPS' should be disabled");
			// assert that the globally set bean is used by the converters
			assertThat(objectMapper).isSameAs(this.objectMapper);
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/sink-channel-configurers.properties")
	public static class TestSink {

	}
}
