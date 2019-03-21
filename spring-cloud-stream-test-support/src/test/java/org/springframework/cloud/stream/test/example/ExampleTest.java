/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.test.example;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that validates that
 * {@link org.springframework.cloud.stream.test.binder.TestSupportBinder} applies
 * correctly.
 */
@RunWith(SpringJUnit4ClassRunner.class)
// @checkstyle:off
@SpringBootTest(classes = ExampleTest.MyProcessor.class, webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"--spring.cloud.stream.bindings.input.contentType=text/plain",
		"--spring.cloud.stream.bindings.output.contentType=text/plain" })
// @checkstyle:on
@DirtiesContext
public class ExampleTest {

	@Autowired
	private MessageCollector messageCollector;

	@Autowired
	private Processor processor;

	@Test
	@SuppressWarnings("unchecked")
	public void testWiring() {
		Message<String> message = new GenericMessage<>("hello");
		this.processor.input().send(message);
		Message<String> received = (Message<String>) this.messageCollector
				.forChannel(this.processor.output()).poll();
		assertThat(received.getPayload()).isEqualTo("hello world");
	}

	@SpringBootApplication
	@EnableBinding(Processor.class)
	public static class MyProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in + " world";
		}

	}

}
