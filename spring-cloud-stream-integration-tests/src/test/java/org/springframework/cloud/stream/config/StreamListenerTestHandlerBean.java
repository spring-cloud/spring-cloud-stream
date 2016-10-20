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
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
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

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@RunWith(Parameterized.class)
public class StreamListenerTestHandlerBean {

	private Class<?> configClass;

	public StreamListenerTestHandlerBean(Class<?> configClass) {
		this.configClass = configClass;
	}

	@Parameterized.Parameters
	public static Collection InputConfigs() {
		return Arrays.asList(new Class[] { TestHandlerBean1.class,
				TestHandlerBean2.class, TestHandlerBean3.class, TestHandlerBean4.class });
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
				MessageBuilder.withPayload("{\"bar\":\"barbar" + id + "\"}")
						.setHeader("contentType", "application/json").build());
		HandlerBean handlerBean = context.getBean(HandlerBean.class);
		assertThat(handlerBean.receivedPojos).hasSize(1);
		assertThat(handlerBean.receivedPojos.get(0)).hasFieldOrPropertyWithValue("bar",
				"barbar" + id);
		Message<String> message = (Message<String>) collector.forChannel(
				processor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("{\"qux\":\"barbar" + id + "\"}");
		assertThat(message.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class)
				.includes(MimeTypeUtils.APPLICATION_JSON));
		context.close();
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean1 {

		@Bean
		public HandlerBean1 handlerBean() {
			return new HandlerBean1();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean2 {

		@Bean
		public HandlerBean2 handlerBean() {
			return new HandlerBean2();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean3 {

		@Bean
		public HandlerBean3 handlerBean() {
			return new HandlerBean3();
		}
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class TestHandlerBean4 {

		@Bean
		public HandlerBean4 handlerBean() {
			return new HandlerBean4();
		}
	}

	public static class HandlerBean1 extends HandlerBean {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean2 extends HandlerBean {

		@StreamListener
		@SendTo(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean3 extends HandlerBean {

		@StreamListener
		@Output(Processor.OUTPUT)
		public BazPojo receive(@Input(Processor.INPUT) FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean4 extends HandlerBean {

		@StreamListener(Processor.INPUT)
		@Output(Processor.OUTPUT)
		public BazPojo receive(FooPojo fooMessage) {
			this.receivedPojos.add(fooMessage);
			BazPojo bazPojo = new BazPojo();
			bazPojo.setQux(fooMessage.getBar());
			return bazPojo;
		}
	}

	public static class HandlerBean {

		List<FooPojo> receivedPojos = new ArrayList<>();

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

	public static class BazPojo {

		private String qux;

		public String getQux() {
			return this.qux;
		}

		public void setQux(String qux) {
			this.qux = qux;
		}
	}

}
