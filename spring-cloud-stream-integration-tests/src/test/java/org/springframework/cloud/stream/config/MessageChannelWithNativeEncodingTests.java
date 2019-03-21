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

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {
		MessageChannelWithNativeEncodingTests.NativeEncodingSource.class })
public class MessageChannelWithNativeEncodingTests {

	@Autowired
	private Source nativeEncodingSource;

	@Autowired
	private MessageCollector messageCollector;

	@Test
	public void testOutboundContentTypeInterceptorIsSkippedWhenNativeEncodingIsEnabled()
			throws Exception {
		this.nativeEncodingSource.output()
				.send(MessageBuilder.withPayload("hello foobar!").build());
		Message<?> message = this.messageCollector
				.forChannel(this.nativeEncodingSource.output()).poll(1, TimeUnit.SECONDS);
		// should not convert the payload to byte[] even though we set a contentType on
		// the channel.
		// This is becasue, we are using native encoding.
		assertThat(message.getPayload()).isInstanceOf(String.class);
		assertThat(message.getPayload()).isEqualTo("hello foobar!");
		assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isNull();
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/native-encoding-source.properties")
	public static class NativeEncodingSource {

	}

}
