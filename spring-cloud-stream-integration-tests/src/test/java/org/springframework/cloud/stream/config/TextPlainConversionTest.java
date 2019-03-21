/*
 * Copyright 2017-2019 the original author or authors.
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
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.TestSupportBinder;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @since 1.2
 */
@RunWith(SpringJUnit4ClassRunner.class)
// @checkstyle:off
@SpringBootTest(classes = TextPlainConversionTest.FooProcessor.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
// @checkstyle:on
public class TextPlainConversionTest {

	@Autowired
	private Processor testProcessor;

	@Autowired
	private BinderFactory binderFactory;

	@Test
	public void testTextPlainConversionOnOutput() throws Exception {
		this.testProcessor.input().send(MessageBuilder.withPayload("Bar").build());
		@SuppressWarnings("unchecked")
		Message<String> received = (Message<String>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testProcessor.output())
						.poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isEqualTo("Foo{name='Bar'}");
	}

	@Test
	public void testByteArrayConversionOnOutput() throws Exception {
		this.testProcessor.output()
				.send(MessageBuilder.withPayload("Bar".getBytes()).build());
		@SuppressWarnings("unchecked")
		Message<String> received = (Message<String>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testProcessor.output())
						.poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isEqualTo("Bar");
	}

	@Test
	public void testTextPlainConversionOnInputAndOutput() throws Exception {
		this.testProcessor.input()
				.send(MessageBuilder.withPayload(new Foo("Bar")).build());
		@SuppressWarnings("unchecked")
		Message<String> received = (Message<String>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testProcessor.output())
						.poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isEqualTo("Foo{name='Foo{name='Bar'}'}");
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/textplain/text-plain.properties")
	public static class FooProcessor {

		@ServiceActivator(inputChannel = "input", outputChannel = "output")
		public Foo consume(String foo) {
			return new Foo(foo);
		}

	}

	public static class Foo {

		private String name;

		public Foo(String name) {
			this.name = name;
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
