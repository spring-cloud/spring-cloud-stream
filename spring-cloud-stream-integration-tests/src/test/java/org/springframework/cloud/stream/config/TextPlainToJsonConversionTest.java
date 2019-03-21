/*
 * Copyright 2017-2018 the original author or authors.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 * @since 1.2
 */
@RunWith(SpringJUnit4ClassRunner.class)
// @checkstyle:off
@SpringBootTest(classes = TextPlainToJsonConversionTest.FooProcessor.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
// @checkstyle:on
public class TextPlainToJsonConversionTest {

	@Autowired
	private Processor testProcessor;

	@Autowired
	private BinderFactory binderFactory;

	private ObjectMapper mapper = new ObjectMapper();

	@SuppressWarnings("unchecked")
	@Test
	public void testNoContentTypeToJsonConversionOnInput() throws Exception {
		this.testProcessor.input()
				.send(MessageBuilder.withPayload("{\"name\":\"Bar\"}").build());
		Message<String> received = (Message<String>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testProcessor.output())
						.poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		Foo foo = this.mapper.readValue(received.getPayload(), Foo.class);
		assertThat(foo.getName()).isEqualTo("transformed-Bar");
	}

	/**
	 * @since 2.0: Conversion from text/plain -> json is no longer supported. Strict
	 * contentType only.
	 */
	@Test(expected = MessagingException.class)
	public void testTextPlainToJsonConversionOnInput() {
		this.testProcessor.input().send(MessageBuilder.withPayload("{\"name\":\"Bar\"}")
				.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain").build());
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class FooProcessor {

		@StreamListener("input")
		@SendTo("output")
		public Foo consume(Foo foo) {
			Foo returnFoo = new Foo();
			returnFoo.setName("transformed-" + foo.getName());
			return returnFoo;
		}

	}

	public static class Foo {

		private String name;

		public Foo() {
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Foo{name='" + this.name + "'}";
		}

	}

}
