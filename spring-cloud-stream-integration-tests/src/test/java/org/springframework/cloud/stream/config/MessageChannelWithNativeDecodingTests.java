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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {
		MessageChannelWithNativeDecodingTests.NativeDecodingSink.class })
public class MessageChannelWithNativeDecodingTests {

	@Autowired
	private Sink nativeDecodingSink;

	@Test
	public void testMessageConverterInterceptorsAreSkippedWhenNativeDecodingIsEnabled()
			throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);

		byte[] serializedData;
		ObjectOutput out;
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			out = new ObjectOutputStream(bos);
			out.writeObject(123);
			out.flush();
			serializedData = bos.toByteArray();
		}

		MessageHandler messageHandler = message -> {
			// ensure that the data is not deserialized becasue of native decoding
			// and the content type set in the properties file didn't take any effect
			assertThat(message.getPayload()).isInstanceOf(byte[].class);
			assertThat(message.getPayload()).isEqualTo(serializedData);
			latch.countDown();
		};
		this.nativeDecodingSink.input().subscribe(messageHandler);

		this.nativeDecodingSink.input()
				.send(MessageBuilder.withPayload(serializedData).build());
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		this.nativeDecodingSink.input().unsubscribe(messageHandler);
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/native-decoding-sink.properties")
	public static class NativeDecodingSink {

	}

}
