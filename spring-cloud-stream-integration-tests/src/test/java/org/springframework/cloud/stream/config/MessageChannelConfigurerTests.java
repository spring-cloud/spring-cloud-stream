/*
 * Copyright 2015-2017 the original author or authors.
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
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

/**
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { MessageChannelConfigurerTests.TestSink.class,
		MessageChannelConfigurerTests.TestSource.class, SpelExpressionConverterConfiguration.class})
public class MessageChannelConfigurerTests {

	@Autowired
	private Sink testSink;

	@Autowired
	private Source testSource;

	@Autowired
	private CompositeMessageConverterFactory messageConverterFactory;

	@Autowired
	private MessageCollector messageCollector;

	@Test
	public void testMessageConverterConfigurer() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		MessageHandler messageHandler = message -> {
			assertThat(message.getPayload()).isInstanceOf(byte[].class);
			assertThat(message.getPayload()).isEqualTo("{\"message\":\"Hi\"}".getBytes());
			latch.countDown();
		};
		testSink.input().subscribe(messageHandler);
		testSink.input().send(MessageBuilder.withPayload("{\"message\":\"Hi\"}".getBytes()).build());
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		testSink.input().unsubscribe(messageHandler);
	}

	@Test
	public void testObjectMapperConfig() throws Exception {
		CompositeMessageConverter converters = (CompositeMessageConverter) messageConverterFactory
				.getMessageConverterForType(MimeTypeUtils.APPLICATION_JSON);
		for (MessageConverter converter : converters.getConverters()) {
			DirectFieldAccessor converterAccessor = new DirectFieldAccessor(converter);
			ObjectMapper objectMapper = (ObjectMapper) converterAccessor.getPropertyValue("objectMapper");
			// assert that the ObjectMapper used by the converters is compliant with the
			// Boot configuration
			assertThat(!objectMapper.getSerializationConfig().isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS))
					.withFailMessage("SerializationFeature 'WRITE_DATES_AS_TIMESTAMPS' should be disabled");
			// assert that the globally set bean is used by the converters
		}
	}

	@Test
	public void testPartitionHeader() throws Exception {
		this.testSource.output().send(MessageBuilder.withPayload("{\"message\":\"Hi\"}").build());
		Message<?> message = this.messageCollector.forChannel(testSource.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message.getHeaders().get(BinderHeaders.PARTITION_HEADER).equals(0));
		assertNull(message.getHeaders().get(BinderHeaders.PARTITION_OVERRIDE));
	}

	@Test
	public void testPartitionHeaderWithPartitionOverride() throws Exception {
		this.testSource.output().send(MessageBuilder.withPayload("{\"message\":\"Hi\"}")
				.setHeader(BinderHeaders.PARTITION_OVERRIDE, 123).build());
		Message<?> message = this.messageCollector.forChannel(testSource.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message.getHeaders().get(BinderHeaders.PARTITION_HEADER).equals(123));
		assertNull(message.getHeaders().get(BinderHeaders.PARTITION_OVERRIDE));
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/sink-channel-configurers.properties")
	public static class TestSink {

	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/partitioned-configurers.properties")
	public static class TestSource {

	}
}
