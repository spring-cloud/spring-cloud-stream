/*
 * Copyright 2019-2020 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
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
		this.context.close();
	}

	@Test
	public void testPathThrough() {
		this.context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(FunctionsConfiguration.class))
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = this.context.getBean(InputDestination.class);
		OutputDestination target = this.context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void testSingleFunction() {
		this.context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(FunctionsConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.definition=toUpperCase",
								"--spring.jmx.enabled=false");

		InputDestination source = this.context.getBean(InputDestination.class);
		OutputDestination target = this.context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("HELLO".getBytes(StandardCharsets.UTF_8));
		//to ensure there is no possibility of load balancing to the EnableBinding
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("HELLO".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void testComposedFunction() {
		this.context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(FunctionsConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.definition=toUpperCase|concatWithSelf",
								"--spring.jmx" + ".enabled=false",
								"--logging.level.org.springframework.integration=DEBUG");

		InputDestination source = this.context.getBean(InputDestination.class);
		OutputDestination target = this.context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		String result = new String(target.receive(1000).getPayload());
		assertThat(result).isEqualTo("HELLO:HELLO");
	}

	@Test
	public void testConsumer() {
		this.context = new SpringApplicationBuilder(TestChannelBinderConfiguration
				.getCompleteConfiguration(ConsumerConfiguration.class))
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.function.definition=log",
								"--spring.jmx.enabled=false");

		InputDestination source = this.context.getBean(InputDestination.class);
		OutputDestination target = this.context.getBean(OutputDestination.class);
		source.send(new GenericMessage<byte[]>("hello".getBytes(StandardCharsets.UTF_8)));
		source.send(
				new GenericMessage<byte[]>("hello1".getBytes(StandardCharsets.UTF_8)));
		source.send(
				new GenericMessage<byte[]>("hello2".getBytes(StandardCharsets.UTF_8)));
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("hello1".getBytes(StandardCharsets.UTF_8));
		assertThat(target.receive(1000).getPayload())
				.isEqualTo("hello2".getBytes(StandardCharsets.UTF_8));
	}

	@EnableAutoConfiguration
	@Import(BaseProcessorConfiguration.class)
	public static class FunctionsConfiguration {

		@Bean
		public Function<String, String> toUpperCase() {
			return v -> {
				System.out.println();
				return v.toUpperCase();
			};
		}

		@Bean
		public Function<String, String> concatWithSelf() {
			return x -> x + ":" + x;
		}

	}

	@EnableAutoConfiguration
	@Import(BaseProcessorConfiguration.class)
	public static class ConsumerConfiguration {

		@SuppressWarnings("unchecked")
		@Bean
		public Consumer<String> log(OutputDestination out) {
			return x -> {
				DirectFieldAccessor dfa = new DirectFieldAccessor(out);
				MessageChannel channel = ((List<MessageChannel>) dfa.getPropertyValue("channels")).get(0);
				channel.send(new GenericMessage<byte[]>(x.getBytes()));
			};
		}

	}

	/**
	 * This configuration essentially emulates our existing app-starters for Processor and
	 * essentially demonstrates how a function(s) could be applied to an existing
	 * processor app via {@link IntegrationFlowFunctionSupport} class.
	 */
	@EnableBinding(Processor.class)
	public static class BaseProcessorConfiguration {

		@Autowired
		private Processor processor;

		@Bean
		public IntegrationFlow fromChannel() {

			return IntegrationFlows.from(this.processor.input())
					.channel(this.processor.output()).get();
		}

	}

}
