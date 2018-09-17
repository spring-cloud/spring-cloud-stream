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
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @since 2.1
 */
public class ProcessorToFunctionsSupportTests {

	private ConfigurableApplicationContext context;

	@After
	public void cleanUp() {
		context.close();
	}

	@Test
	public void testPathThrough() {
		context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionsConfiguration.class)).web(
			WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	@Ignore
	public void testSingleFunction() {
		context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionsConfiguration.class)).web(
			WebApplicationType.NONE)
			.run("--spring.cloud.stream.function.definition=toUpperCase", "--spring.jmx.enabled=false");

		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload()).isEqualTo("HELLO".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	@Ignore
	public void testComposedFunction() {
	context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(FunctionsConfiguration.class)).web(
			WebApplicationType.NONE)
			.run("--spring.cloud.stream.function.definition=toUpperCase|concatWithSelf",
				"--spring.jmx" + ".enabled=false", "--logging.level.org.springframework.integration=DEBUG");

		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload()).isEqualTo("HELLO:HELLO".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void testConsumer() {
		context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(ConsumerConfiguration.class)).web(
			WebApplicationType.NONE).run("--spring.cloud.stream.function.definition=log", "--spring.jmx.enabled=false");

		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		source.send(new GenericMessage<byte[]>("hello1".getBytes(StandardCharsets.UTF_8)));
		source.send(new GenericMessage<byte[]>("hello2".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(1000).getPayload()).isEqualTo("hello1".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(1000).getPayload()).isEqualTo("hello2".getBytes(StandardCharsets.UTF_8));
	}

	@EnableAutoConfiguration
	@Import(BaseProcessorConfiguration.class)
	public static class FunctionsConfiguration {

		@Bean
		public Function<String, String> toUpperCase() {
			return String::toUpperCase;
		}

		@Bean
		public Function<String, String> concatWithSelf() {
			return x -> x + ":" + x;
		}
	}

	@EnableAutoConfiguration
	@Import(BaseProcessorConfiguration.class)
	public static class ConsumerConfiguration {

		@Autowired
		OutputDestination out;

		@Bean
		public Consumer<String> log() {
			return x -> {
				DirectFieldAccessor dfa = new DirectFieldAccessor(out);
				MessageChannel channel = (MessageChannel) dfa.getPropertyValue("channel");
				channel.send(new GenericMessage<byte[]>(x.getBytes()));
			};
		}
	}

	/**
	 * This configuration essentially emulates our existing app-starters for Processor
	 * and essentially demonstrates how a function(s) could be applied to an existing
	 * processor app via {@link IntegrationFlowFunctionSupport} class.
	 */
	@EnableBinding(Processor.class)
	public static class BaseProcessorConfiguration {

		@Autowired
		private Processor processor;

		@Bean
		public IntegrationFlow fromChannel() {

			return IntegrationFlows.from(processor.input())
				.channel(processor.output()).get();
		}

	}
}
