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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@RunWith(Parameterized.class)
public class StreamListenerTestContentTypeConversion {

	private Class<?> configClass;

	public StreamListenerTestContentTypeConversion(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection InputConfigs() {
		return Arrays.asList(new Class[] { TestSink1.class, TestSink2.class });
	}

	@Test
	public void testContentTypeConversion() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
				"--server.port=0");
		@SuppressWarnings("unchecked")
		TestSink testSink = context.getBean(TestSink.class);
		Sink sink = context.getBean(Sink.class);
		String id = UUID.randomUUID().toString();
		sink.input().send(
				MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		assertThat(testSink.latch.await(10, TimeUnit.SECONDS));
		assertThat(testSink.receivedArguments).hasSize(1);
		assertThat(testSink.receivedArguments.get(0)).hasFieldOrPropertyWithValue("bar",
				"barbar" + id);
		context.close();
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestSink1 extends TestSink {

		@StreamListener(Sink.INPUT)
		public void receive(FooPojo fooPojo) {
			this.receivedArguments.add(fooPojo);
			this.latch.countDown();
		}
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	public static class TestSink2 extends TestSink {

		@StreamListener
		public void receive(@Input(Processor.INPUT) FooPojo fooPojo) {
			this.receivedArguments.add(fooPojo);
			this.latch.countDown();
		}
	}

	public static class TestSink {
		List<FooPojo> receivedArguments = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(1);
	}

	public static class FooPojo {

		private String bar;

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}
	}

}
