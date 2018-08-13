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

import org.assertj.core.api.Assertions;
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
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@RunWith(Parameterized.class)
public class StreamListenerHandlerBeanTests {

	private Class<?> configClass;

	public StreamListenerHandlerBeanTests(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection<?> InputConfigs() {
		return Arrays.asList(TestHandlerBeanWithSendTo.class, TestHandlerBean2.class);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testHandlerBean() throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(this.configClass,
				"--spring.cloud.stream.bindings.output.contentType=application/json",
				"--server.port=0");
		MessageCollector collector = context.getBean(MessageCollector.class);
		Processor processor = context.getBean(Processor.class);
		String id = UUID.randomUUID().toString();
		processor.input().send(
				MessageBuilder.withPayload("{\"foo\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		HandlerBean handlerBean = context.getBean(HandlerBean.class);
		Assertions.assertThat(handlerBean.receivedPojos).hasSize(1);
		Assertions.assertThat(handlerBean.receivedPojos.get(0)).hasFieldOrPropertyWithValue("foo",
				"barbar" + id);
		Message<String> message = (Message<String>) collector.forChannel(
				processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("{\"bar\":\"barbar" + id + "\"}");
		assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
				.includes(MimeTypeUtils.APPLICATION_JSON));
		context.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBeanWithSendTo {

		@Bean
		public HandlerBeanWithSendTo handlerBean() {
			return new HandlerBeanWithSendTo();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean2 {

		@Bean
		public HandlerBeanWithOutput handlerBean() {
			return new HandlerBeanWithOutput();
		}
	}

	public static class HandlerBeanWithSendTo extends HandlerBean {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public StreamListenerTestUtils.BarPojo receive(StreamListenerTestUtils.FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			StreamListenerTestUtils.BarPojo barPojo = new StreamListenerTestUtils.BarPojo();
			barPojo.setBar(fooMessage.getFoo());
			return barPojo;
		}
	}

	public static class HandlerBeanWithOutput extends HandlerBean {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public StreamListenerTestUtils.BarPojo receive(StreamListenerTestUtils.FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			StreamListenerTestUtils.BarPojo barPojo = new StreamListenerTestUtils.BarPojo();
			barPojo.setBar(fooMessage.getFoo());
			return barPojo;
		}
	}

	public static class HandlerBean {

		List<StreamListenerTestUtils.FooPojo> receivedPojos = new ArrayList<>();

	}

}
