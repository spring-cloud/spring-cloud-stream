/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test validates proper function binding for applications where EnableBinding is declared.
 *
 * @author Oleg Zhurakousky
 */
public class GreenfieldFunctionEnableBindingTests {

	@Test
	public void testSourceFromSupplier() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SourceFromSupplier.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.definition=date", "--spring.jmx.enabled=false")) {

			OutputDestination target = context.getBean(OutputDestination.class);
			Message<byte[]> sourceMessage = target.receive(10000);
			Date date = (Date) new CompositeMessageConverterFactory().getMessageConverterForAllRegistered().fromMessage(sourceMessage, Date.class);
			assertThat(date).isEqualTo(new Date(12345L));

			sourceMessage = target.receive(10000);
			date = (Date) new CompositeMessageConverterFactory().getMessageConverterForAllRegistered().fromMessage(sourceMessage, Date.class);
			assertThat(date).isEqualTo(new Date(12345L));
		}
	}

	@Test
	public void testProcessorFromFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ProcessorFromFunction.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.definition=toUpperCase", "--spring.jmx.enabled=false")) {

			InputDestination source = context.getBean(InputDestination.class);
			source.send(new GenericMessage<byte[]>("John Doe".getBytes()));
			OutputDestination target = context.getBean(OutputDestination.class);
			assertThat(target.receive(10000).getPayload()).isEqualTo("JOHN DOE".getBytes(StandardCharsets.UTF_8));
		}
	}

	@Test
	public void testSinkFromConsumer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(SinkFromConsumer.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.definition=sink", "--spring.jmx.enabled=false")) {

			InputDestination source = context.getBean(InputDestination.class);
			PollableChannel result = context.getBean("result", PollableChannel.class);
			source.send(new GenericMessage<byte[]>("John Doe".getBytes()));
			assertThat(result.receive(10000).getPayload()).isEqualTo("John Doe");
		}
	}


	@EnableAutoConfiguration
	@EnableBinding(Source.class)
	public static class SourceFromSupplier {
		@Bean
		public Supplier<Date> date() {
			return () -> new Date(12345L);
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class ProcessorFromFunction {
		@Bean
		public Function<String, String> toUpperCase() {
			return s -> s.toUpperCase();
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Sink.class)
	public static class SinkFromConsumer {
		@Bean
		public PollableChannel result() {
			return new QueueChannel();
		}
		@Bean
		public Consumer<String> sink(PollableChannel result) {
			return s -> {
				result.send(new GenericMessage<String>(s));
				System.out.println(s);
			};
		}
	}
}
