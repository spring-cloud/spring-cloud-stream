/*
 * Copyright 2015-2017 the original author or authors.
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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { MessageChannelConfigurerTests.TestSink.class,
		MessageChannelConfigurerTests.TestSource.class,
		SpelExpressionConverterConfiguration.class })
public class MessageChannelConfigurerTests {

	@Autowired
	private Sink testSink;

	@Autowired
	private Source testSource;

	@Autowired
	private MessageCollector messageCollector;

	@Test
	public void testChannelTypes() throws Exception {
		DirectWithAttributesChannel inputChannel = (DirectWithAttributesChannel) this.testSink
				.input();
		DirectWithAttributesChannel outputChannel = (DirectWithAttributesChannel) this.testSource
				.output();
		assertThat(inputChannel.getAttribute("type")).isEqualTo(Sink.INPUT);
		assertThat(outputChannel.getAttribute("type")).isEqualTo(Source.OUTPUT);
	}

	@Test
	public void testMessageConverterConfigurer() throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		MessageHandler messageHandler = message -> {
			assertThat(message.getPayload()).isInstanceOf(byte[].class);
			assertThat(message.getPayload()).isEqualTo("{\"message\":\"Hi\"}".getBytes());
			latch.countDown();
		};
		this.testSink.input().subscribe(messageHandler);
		this.testSink.input().send(
				MessageBuilder.withPayload("{\"message\":\"Hi\"}".getBytes()).build());
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		this.testSink.input().unsubscribe(messageHandler);
	}

	@Test
	public void testPartitionHeader() throws Exception {
		this.testSource.output()
				.send(MessageBuilder.withPayload("{\"message\":\"Hi\"}").build());
		Message<?> message = this.messageCollector.forChannel(this.testSource.output())
				.poll(1, TimeUnit.SECONDS);
		assertThat(message.getHeaders().get(BinderHeaders.PARTITION_HEADER).equals(0));
		assertThat(message.getHeaders().get(BinderHeaders.PARTITION_OVERRIDE)).isNull();
	}

	@Test
	public void testPartitionHeaderWithPartitionOverride() throws Exception {
		this.testSource.output().send(MessageBuilder.withPayload("{\"message\":\"Hi\"}")
				.setHeader(BinderHeaders.PARTITION_OVERRIDE, 123).build());
		Message<?> message = this.messageCollector.forChannel(this.testSource.output())
				.poll(1, TimeUnit.SECONDS);
		assertThat(message.getHeaders().get(BinderHeaders.PARTITION_HEADER).equals(123));
		assertThat(message.getHeaders().get(BinderHeaders.PARTITION_OVERRIDE)).isNull();
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
