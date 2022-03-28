/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerContentTypeConversionTests {

	@Test
	public void testContentTypeConversion() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(TestSinkWithContentTypeConversion.class, "--server.port=0");
		@SuppressWarnings("unchecked")
		TestSinkWithContentTypeConversion testSink = context
				.getBean(TestSinkWithContentTypeConversion.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
				.setHeader("contentType", "application/json").build());
		assertThat(testSink.latch.await(10, TimeUnit.SECONDS));
		assertThat(testSink.receivedArguments).hasSize(1);
		assertThat(testSink.receivedArguments.get(0)).hasFieldOrPropertyWithValue("foo",
				"barbar" + id);
		context.close();
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestSinkWithContentTypeConversion {

		List<StreamListenerTestUtils.FooPojo> receivedArguments = new ArrayList<>();

		CountDownLatch latch = new CountDownLatch(1);

		@StreamListener(Sink.INPUT)
		public void receive(StreamListenerTestUtils.FooPojo fooPojo) {
			this.receivedArguments.add(fooPojo);
			this.latch.countDown();
		}

	}

}
