/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@RunWith(Parameterized.class)
public class StreamListenerMessageArgumentTests {

	private Class<?> configClass;

	public StreamListenerMessageArgumentTests(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection<?> InputConfigs() {
		return Arrays.asList(new Class[] { TestPojoWithMessageArgument1.class, TestPojoWithMessageArgument2.class });
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMessageArgument() throws Exception {
		ConfigurableApplicationContext context = SpringApplication
				.run(this.configClass, "--server.port=0", "--spring.cloud.stream.bindings.output.contentType=text/plain","--spring.jmx.enabled=false");
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input().send(MessageBuilder.withPayload("barbar" + id)
				.setHeader("contentType", "text/plain").build());
		TestPojoWithMessageArgument testPojoWithMessageArgument = context
				.getBean(TestPojoWithMessageArgument.class);
		assertThat(testPojoWithMessageArgument.receivedMessages).hasSize(1);
		assertThat(testPojoWithMessageArgument.receivedMessages.get(0).getPayload()).isEqualTo("barbar" + id);
		Message<String> message = (Message<String>) collector
				.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).contains("barbar" + id);
		context.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument1 extends TestPojoWithMessageArgument {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public StreamListenerTestUtils.BarPojo receive(Message<String> fooMessage) {
			this.receivedMessages.add(fooMessage);
			StreamListenerTestUtils.BarPojo barPojo = new StreamListenerTestUtils.BarPojo();
			barPojo.setBar(fooMessage.getPayload());
			return barPojo;
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestPojoWithMessageArgument2 extends TestPojoWithMessageArgument {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public StreamListenerTestUtils.BarPojo receive(Message<String> fooMessage) {
			this.receivedMessages.add(fooMessage);
			StreamListenerTestUtils.BarPojo barPojo = new StreamListenerTestUtils.BarPojo();
			barPojo.setBar(fooMessage.getPayload());
			return barPojo;
		}
	}

	public static class TestPojoWithMessageArgument {
		List<Message<String>> receivedMessages = new ArrayList<>();
	}

}
