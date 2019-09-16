/*
 * Copyright 2019-2019 the original author or authors.
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

import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;


/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class MultipleInputOutputFunctionTests {

	@Test(expected = BeanCreationException.class)
	public void testFailureWithNonReactiveFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=multipleInputNonReactive")) {
			context.getBean(InputDestination.class);
		}
	}

	@Test(expected = BeanCreationException.class)
	public void testFailureWithReactiveArrayOutput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=multiReactiveInputReactiveArrayOutput")) {
			context.getBean(InputDestination.class);
		}
	}

	@Test(expected = BeanCreationException.class)
	public void testFailureWithReactiveArrayOutputNonGeneric() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=multiReactiveInputReactiveArrayOutputNoGeneric")) {
			context.getBean(InputDestination.class);
		}
	}

	@Test(expected = BeanCreationException.class)
	public void testFailureWithReactiveArrayInput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=genericReactiveArrayInput")) {
			context.getBean(InputDestination.class);
		}
	}

	@Test(expected = BeanCreationException.class)
	public void testFailureWithReactiveArrayInputNonGeneric() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=nonGenericReactiveArrayInput")) {
			context.getBean(InputDestination.class);
		}
	}

	@Test(expected = BeanCreationException.class)
	public void testFailureWithConsumer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=multiInputConsumer")) {
			context.getBean(InputDestination.class);
		}
	}

	@Test
	public void testMultiInputSingleOutput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=multiInputSingleOutput")) {
			context.getBean(InputDestination.class);

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> stringInputMessage = MessageBuilder.withPayload("one".getBytes()).build();
			Message<byte[]> integerInputMessage = MessageBuilder.withPayload("1".getBytes()).build();
			inputDestination.send(stringInputMessage, 0);
			inputDestination.send(integerInputMessage, 1);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("one".getBytes());
			outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("1".getBytes());
		}
	}

	@Test
	public void testSingleInputMultiOutput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=singleInputMultipleOutputs")) {
			context.getBean(InputDestination.class);

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			for (int i = 0; i < 10; i++) {
				inputDestination.send(MessageBuilder.withPayload(String.valueOf(i).getBytes()).build());
			}

			int counter = 0;
			for (int i = 0; i < 5; i++) {
				Message<byte[]> even = outputDestination.receive(0, 0);
				assertThat(even.getPayload()).isEqualTo(("EVEN: " + String.valueOf(counter++)).getBytes());
				Message<byte[]> odd = outputDestination.receive(0, 1);
				assertThat(odd.getPayload()).isEqualTo(("ODD: " + String.valueOf(counter++)).getBytes());
			}
		}
	}

	@Test
	public void testMultipleFunctions() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=uppercase;reverse")) {
			context.getBean(InputDestination.class);

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage, 0);
			inputDestination.send(inputMessage, 1);

			Message<byte[]> outputMessage = outputDestination.receive(0, 0);
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());

			outputMessage = outputDestination.receive(0, 1);
			assertThat(outputMessage.getPayload()).isEqualTo("olleH".getBytes());
		}
	}

	@Test
	public void testMultipleFunctionsWithComposition() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ReactiveFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
									"--spring.cloud.function.definition=uppercase|reverse;reverse|uppercase")) {
			context.getBean(InputDestination.class);

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage, 0);
			inputDestination.send(inputMessage, 1);

			Message<byte[]> outputMessage = outputDestination.receive(0, 0);
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());

			outputMessage = outputDestination.receive(0, 1);
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());
		}
	}

	@EnableAutoConfiguration
	public static class ReactiveFunctionConfiguration {

		@Bean
		public Function<String, String> uppercase() {
			return value -> value.toUpperCase();
		}

		@Bean
		public Function<String, String> reverse() {
			return value -> new StringBuilder(value).reverse().toString();
		}

		@Bean
		public Function<Tuple2<String, String>, String> multipleInputNonReactive() { // not supported
			return tuple -> null;
		}

		@Bean
		public Function<Tuple2<Flux<String>, Flux<Integer>>, Flux<?>[]> multiReactiveInputReactiveArrayOutput() { // not supported
			return tuple -> null;
		}

		@Bean
		public Consumer<Tuple2<Flux<String>, Flux<Integer>>> multiInputConsumer() { // not supported
			return tuple -> System.out.println();
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public Function<Tuple2<Flux<String>, Flux<Integer>>, Flux[]> multiReactiveInputReactiveArrayOutputNoGeneric() { // not supported
			return tuple -> null;
		}

		@Bean
		public Function<Flux<?>[], Tuple2<Flux<String>, Flux<Integer>>> genericReactiveArrayInput() { // not supported
			return tuple -> null;
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public Function<Flux[], Tuple2<Flux<String>, Flux<Integer>>> nonGenericReactiveArrayInput() { // not supported
			return tuple -> null;
		}

		@Bean
		public  Function<Tuple2<Flux<String>, Flux<Integer>>, Flux<String>> multiInputSingleOutput() {
			return tuple -> {
				Flux<String> stringStream = tuple.getT1();
				Flux<String> intStream = tuple.getT2().map(i -> String.valueOf(i));
				return Flux.merge(stringStream, intStream);
			};
		}

		@Bean
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public static Function<Flux<Integer>, Tuple2<Flux<String>, Flux<String>>> singleInputMultipleOutputs() {
			return flux -> {
				Flux<Integer> connectedFlux = flux.publish().autoConnect(2);
				UnicastProcessor even = UnicastProcessor.create();
				UnicastProcessor odd = UnicastProcessor.create();
				Flux<Integer> evenFlux = connectedFlux.filter(number -> number % 2 == 0).doOnNext(number -> even.onNext("EVEN: " + number));
				Flux<Integer> oddFlux = connectedFlux.filter(number -> number % 2 != 0).doOnNext(number -> odd.onNext("ODD: " + number));

				return Tuples.of(Flux.from(even).doOnSubscribe(x -> evenFlux.subscribe()), Flux.from(odd).doOnSubscribe(x -> oddFlux.subscribe()));
			};
		}
	}
}
