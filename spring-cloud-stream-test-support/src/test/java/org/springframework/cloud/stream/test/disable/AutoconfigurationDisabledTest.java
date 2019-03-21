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

package org.springframework.cloud.stream.test.disable;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AutoconfigurationDisabledTest.MyProcessor.class, properties = {
		"server.port=-1", "spring.cloud.stream.defaultBinder=test",
		"--spring.cloud.stream.bindings.input.contentType=text/plain",
		"--spring.cloud.stream.bindings.output.contentType=text/plain" })
@DirtiesContext
public class AutoconfigurationDisabledTest {

	@Autowired
	public MessageCollector messageCollector;

	@Autowired
	public Processor processor;

	@SuppressWarnings("unchecked")
	@Test
	public void testAutoconfigurationDisabled() throws Exception {
		this.processor.input().send(MessageBuilder.withPayload("Hello").build());
		// Since the interaction is synchronous, the result should be immediate
		Message<String> response = (Message<String>) this.messageCollector
				.forChannel(this.processor.output()).poll(1000, TimeUnit.MILLISECONDS);
		assertThat(response).isNotNull();
		assertThat(response.getPayload()).isEqualTo("Hello world");
	}

	@SpringBootApplication(exclude = TestSupportBinderAutoConfiguration.class)
	@EnableBinding(Processor.class)
	public static class MyProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in + " world";
		}

	}

}
