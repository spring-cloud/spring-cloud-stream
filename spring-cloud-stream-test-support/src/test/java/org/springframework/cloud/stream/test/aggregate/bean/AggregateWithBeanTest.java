/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.test.aggregate.bean;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.aggregate.AggregateApplication;
import org.springframework.cloud.stream.aggregate.AggregateApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AggregateWithBeanTest.ChainedProcessors.class, properties = {
		"server.port=-1", "--spring.cloud.stream.bindings.input.contentType=text/plain",
		"--spring.cloud.stream.bindings.output.contentType=text/plain" })
@Ignore
public class AggregateWithBeanTest {

	@Autowired
	public MessageCollector messageCollector;

	@Autowired
	public AggregateApplication aggregateApplication;

	@Test
	@SuppressWarnings("unchecked")
	public void testAggregateApplication() throws InterruptedException {
		Processor uppercaseProcessor = this.aggregateApplication
				.getBinding(Processor.class, "upper");
		Processor suffixProcessor = this.aggregateApplication.getBinding(Processor.class,
				"suffix");
		uppercaseProcessor.input().send(MessageBuilder.withPayload("Hello").build());
		Message<String> receivedMessage = (Message<String>) this.messageCollector
				.forChannel(suffixProcessor.output()).poll(1, TimeUnit.SECONDS);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo("HELLO WORLD!");
	}

	@SpringBootApplication
	@EnableBinding
	public static class ChainedProcessors {

		@Bean
		public AggregateApplication aggregateApplication() {
			return new AggregateApplicationBuilder().from(UppercaseProcessor.class)
					.namespace("upper").to(SuffixProcessor.class).namespace("suffix")
					.build();
		}

	}

	@Configuration
	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class UppercaseProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in.toUpperCase();
		}

	}

	@Configuration
	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	public static class SuffixProcessor {

		@Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public String transform(String in) {
			return in + " WORLD!";
		}

	}

}
