/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.test.aggregate.main;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.aggregate.AggregateApplication;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Artem Bilan
 */
@Ignore
public class AggregateWithMainTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testAggregateApplication() throws InterruptedException {
		// emulate a main method
		ConfigurableApplicationContext context = new AggregateApplicationBuilder(
				MainConfiguration.class).web(false).from(UppercaseProcessor.class)
						.namespace("upper").to(SuffixProcessor.class).namespace("suffix")
						.run("--spring.cloud.stream.bindings.input.contentType=text/plain",
								"--spring.cloud.stream.bindings.output.contentType=text/plain");

		AggregateApplication aggregateAccessor = context
				.getBean(AggregateApplication.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Processor uppercaseProcessor = aggregateAccessor.getBinding(Processor.class,
				"upper");
		Processor suffixProcessor = aggregateAccessor.getBinding(Processor.class,
				"suffix");
		uppercaseProcessor.input().send(MessageBuilder.withPayload("Hello").build());
		Message<String> receivedMessage = (Message<String>) messageCollector
				.forChannel(suffixProcessor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo("HELLO WORLD!");
		context.close();
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	public static class MainConfiguration {

	}

	@Configuration
	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	static class UppercaseProcessor {

		@Autowired
		Processor processor;

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in.toUpperCase();
		}

	}

	@Configuration
	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	static class SuffixProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in + " WORLD!";
		}

	}

}
