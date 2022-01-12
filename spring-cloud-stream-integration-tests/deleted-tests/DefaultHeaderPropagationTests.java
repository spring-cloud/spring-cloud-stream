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
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 */
@RunWith(SpringJUnit4ClassRunner.class)
// @checkstyle:off
@SpringBootTest(classes = DefaultHeaderPropagationTests.HeaderPropagationProcessor.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class DefaultHeaderPropagationTests {

	// @checkstyle:on
	@Autowired
	private Processor testProcessor;

	@Autowired
	private BinderFactory binderFactory;

	@Test
	public void testDefaultHeaderPropagation() throws Exception {
		this.testProcessor.input().send(MessageBuilder.withPayload("{'name':'foo'}")
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/json")
				.setHeader("foo", "fooValue").setHeader("bar", "barValue").build());
		@SuppressWarnings("unchecked")
		Message<String> received = (Message<String>) ((TestSupportBinder) this.binderFactory
				.getBinder(null, MessageChannel.class)).messageCollector()
						.forChannel(this.testProcessor.output())
						.poll(1, TimeUnit.SECONDS);
		assertThat(received).isNotNull();
		assertThat(received.getHeaders()).containsEntry("foo", "fooValue");
		assertThat(received.getHeaders()).containsEntry("bar", "barValue");
		assertThat(received.getHeaders()).containsKeys(MessageHeaders.CONTENT_TYPE);
		assertThat(received.getPayload()).isEqualTo("{'name':'foo'}");
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class HeaderPropagationProcessor {

		@ServiceActivator(inputChannel = "input", outputChannel = "output")
		public Message<String> consume(String data) {
			return MessageBuilder.withPayload(data)
					.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain").build();
		}

	}

}
