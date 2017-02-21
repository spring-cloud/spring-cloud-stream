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

package org.springframework.cloud.stream.test.aggregate;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.aggregate.AggregateApplication;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
public class AggregateWithMainTest {

	@Test
	public void testAggregateApplication() throws InterruptedException {
		// emulate a main method
		ConfigurableApplicationContext context = new AggregateApplicationBuilder(TestSupportBinderAutoConfiguration.class).from(UppercaseProcessor.class)
				.namespace("upper").to(SuffixProcessor.class).namespace("suffix").run();

		AggregateApplication aggregateAccessor = context.getBean(AggregateApplication.class);
		MessageCollector messageCollector = context.getBean(MessageCollector.class);
		Processor uppercaseProcessor = aggregateAccessor.getBinding(Processor.class, "upper");
		Processor suffixProcessor = aggregateAccessor.getBinding(Processor.class, "suffix");
		uppercaseProcessor.input().send(MessageBuilder.withPayload("Hello").build());
		Message<?> receivedMessage = messageCollector.forChannel(suffixProcessor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo("HELLO WORLD!");
		context.close();
	}

	@Configuration
	@EnableBinding(Processor.class)
	public static class UppercaseProcessor {


		@Autowired
		Processor processor;

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in.toUpperCase();
		}
	}

	@Configuration
	@EnableBinding(Processor.class)
	public static class SuffixProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in + " WORLD!";
		}
	}
}
