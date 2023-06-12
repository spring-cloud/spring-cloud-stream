/*
 * Copyright 2019-2022 the original author or authors.
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
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
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
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class MultipleInputOutputFunctionTests {

	@Test
	void testFailureWithNonReactiveFunction() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
					ReactiveFunctionConfiguration.class))
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false",
					"--spring.cloud.function.definition=multipleInputNonReactive"));
	}

	@Test
	void testFailureWithReactiveArrayOutput() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
					ReactiveFunctionConfiguration.class))
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false",
					"--spring.cloud.function.definition=multiReactiveInputReactiveArrayOutput"));
	}

	@Test
	void testFailureWithReactiveArrayOutputNonGeneric() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
					ReactiveFunctionConfiguration.class))
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false",
					"--spring.cloud.function.definition=multiReactiveInputReactiveArrayOutputNoGeneric"));
	}

	@Test
	void testFailureWithReactiveArrayInput() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
					ReactiveFunctionConfiguration.class))
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false",
					"--spring.cloud.function.definition=genericReactiveArrayInput"));
	}

	@Test
	void testFailureWithReactiveArrayInputNonGeneric() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
					ReactiveFunctionConfiguration.class))
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false",
					"--spring.cloud.function.definition=nonGenericReactiveArrayInput"));
	}

	@Test
	void testFailureWithConsumer() {
		assertThatExceptionOfType(BeanCreationException.class)
			.isThrownBy(() -> new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
					ReactiveFunctionConfiguration.class))
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false",
					"--spring.cloud.function.definition=multiInputConsumer"));
	}

	@Test
	void testMultiInputSingleOutput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=multiInputSingleOutput")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> stringInputMessage = MessageBuilder.withPayload("one".getBytes()).build();
			Message<byte[]> integerInputMessage = MessageBuilder.withPayload("1".getBytes()).build();
			inputDestination.send(stringInputMessage, "multiInputSingleOutput-in-0");
			inputDestination.send(integerInputMessage, "multiInputSingleOutput-in-1");

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("one".getBytes());
			outputMessage = outputDestination.receive(0, "multiInputSingleOutput-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("1".getBytes());
		}
	}

	@Test
	void testMultiInputMessageSingleOutput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=multiInputSingleOutputMessage")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> stringInputMessage = MessageBuilder.withPayload("one".getBytes()).build();
			Message<byte[]> integerInputMessage = MessageBuilder.withPayload("1".getBytes()).build();
			inputDestination.send(stringInputMessage, "multiInputSingleOutputMessage-in-0");
			inputDestination.send(integerInputMessage, "multiInputSingleOutputMessage-in-1");

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("one".getBytes());
			outputMessage = outputDestination.receive(0, "multiInputSingleOutputMessage-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("1".getBytes());
		}
	}

	@Test
	void testSingleInputMultiOutput() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=singleInputMultipleOutputs",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-0.producer.partitionKeyExpression=payload",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-0.producer.partitionCount=10",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-1.producer.partitionKeyExpression=payload",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-1.producer.partitionCount=10")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			for (int i = 0; i < 10; i++) {
				inputDestination.send(MessageBuilder.withPayload(String.valueOf(i).getBytes()).build(), "singleInputMultipleOutputs-in-0");
			}

			int counter = 0;
			for (int i = 0; i < 5; i++) {
				Message<byte[]> even = outputDestination.receive(0, "singleInputMultipleOutputs-out-0");
				assertThat(even.getPayload()).isEqualTo(("EVEN: " + String.valueOf(counter++)).getBytes());
				Message<byte[]> odd = outputDestination.receive(0, "singleInputMultipleOutputs-out-1");
				assertThat(odd.getPayload()).isEqualTo(("ODD: " + String.valueOf(counter++)).getBytes());
			}
		}
	}

	@Test
	void testMultipleFunctions() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase;reverse")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage, "uppercase-in-0");
			inputDestination.send(inputMessage, "reverse-in-0");

			Message<byte[]> outputMessage = outputDestination.receive(0, "uppercase-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());

			outputMessage = outputDestination.receive(0, "reverse-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("olleH".getBytes());
		}
	}

	@Test
	void testMultipleFunctionsWithComposition() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ReactiveFunctionConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=uppercase|reverse;reverse|uppercase")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessage, "uppercasereverse-in-0");
			inputDestination.send(inputMessage, "reverseuppercase-in-0");

			Message<byte[]> outputMessage = outputDestination.receive(0, "uppercasereverse-out-0");
			System.out.println(new String(outputMessage.getPayload()));
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());

			outputMessage = outputDestination.receive(0, "reverseuppercase-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("OLLEH".getBytes());
		}
	}

	@Test
	void testMultiInputSingleOutputWithCustomContentType() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ContentTypeConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=multiInputSingleOutput",
				"--spring.cloud.stream.bindings.multiInputSingleOutput-in-0.content-type=string/person",
				"--spring.cloud.stream.bindings.multiInputSingleOutput-in-1.content-type=string/employee")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> stringInputMessage = MessageBuilder.withPayload("ricky".getBytes()).build();
			Message<byte[]> integerInputMessage = MessageBuilder.withPayload("bobby".getBytes()).build();
			inputDestination.send(stringInputMessage, "multiInputSingleOutput-in-0");
			inputDestination.send(integerInputMessage, "multiInputSingleOutput-in-1");

			Message<byte[]> outputMessage = outputDestination.receive(1000, "multiInputSingleOutput-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("RICKY".getBytes());
			outputMessage = outputDestination.receive(1000, "multiInputSingleOutput-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("BOBBY".getBytes());
		}
	}

	@Test
	void testMultiInputSingleOutputWithCustomContentType2() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				ContentTypeConfiguration.class))
			.web(WebApplicationType.NONE)
			.run("--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=multiInputSingleOutput2",
				"--spring.cloud.stream.bindings.multiInputSingleOutput2-in-0.content-type=string/person",
				"--spring.cloud.stream.bindings.multiInputSingleOutput2-in-1.content-type=string/employee",
				"--spring.cloud.stream.bindings.multiInputSingleOutput2-out-0.content-type=string/person")) {
			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);

			Message<byte[]> stringInputMessage = MessageBuilder.withPayload("ricky".getBytes()).build();
			Message<byte[]> integerInputMessage = MessageBuilder.withPayload("bobby".getBytes()).build();
			inputDestination.send(stringInputMessage, "multiInputSingleOutput2-in-0");
			inputDestination.send(integerInputMessage, "multiInputSingleOutput2-in-1");

			Message<byte[]> outputMessage = outputDestination.receive(1000, "multiInputSingleOutput2-out-0");
			assertThat(outputMessage.getPayload()).isEqualTo("rickybobby".getBytes());
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
		public  Function<Tuple2<Flux<Message<String>>, Flux<Message<Integer>>>, Flux<String>> multiInputSingleOutputMessage() {
			return tuple -> {
				Flux<String> stringStream = tuple.getT1().map(m -> m.getPayload());
				Flux<String> intStream = tuple.getT2().map(i -> {
					int v = i.getPayload();
					return String.valueOf(v);
				});
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

	@EnableAutoConfiguration
	public static class ContentTypeConfiguration {

		@Bean
		public Function<Tuple2<Flux<Person>, Flux<Employee>>, Flux<String>> multiInputSingleOutput() {
			return tuple -> {
				Flux<String> stringStream = tuple.getT1().map(p -> p.getName().toUpperCase());
				Flux<String> intStream = tuple.getT2().map(p -> p.getName().toUpperCase());
				return Flux.merge(stringStream, intStream);
			};
		}

		@Bean
		public Function<Tuple2<Flux<Person>, Flux<Employee>>, Flux<Person>> multiInputSingleOutput2() {
			return tuple -> {
				return Flux.merge(tuple.getT1(), tuple.getT2()).buffer(Duration.ofMillis(1000)).map(list -> {
					String personName = ((Person) list.get(0)).getName();
					String employeeName = ((Employee) list.get(1)).getName();
					return new Person(personName + employeeName);
				});
			};
		}

		@Bean
		public MessageConverter stringToPersonConverter() {
			return new AbstractMessageConverter(MimeType.valueOf("string/person")) {

				@Override
				protected boolean supports(Class<?> clazz) {
					return Person.class.isAssignableFrom(clazz);
				}

				@Override
				@Nullable
				protected Object convertFromInternal(
					Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
					String name = new String(((byte[]) message.getPayload()), StandardCharsets.UTF_8);
					Person person = new Person(name);
					return person;
				}

				@Override
				@Nullable
				protected Object convertToInternal(
					Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {

					return ((Person) payload).getName().getBytes(StandardCharsets.UTF_8);
				}
			};
		}

		@Bean
		public MessageConverter stringToEmployeeConverter() {
			return new AbstractMessageConverter(MimeType.valueOf("string/employee")) {

				@Override
				protected boolean supports(Class<?> clazz) {
					return Employee.class.isAssignableFrom(clazz);
				}

				@Override
				@Nullable
				protected Object convertFromInternal(
					Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
					String name = new String(((byte[]) message.getPayload()), StandardCharsets.UTF_8);
					Employee person = new Employee(name);
					return person;
				}

				@Override
				@Nullable
				protected Object convertToInternal(
					Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {

					return ((Employee) payload).getName().getBytes(StandardCharsets.UTF_8);
				}
			};
		}
	}

	private static class Person {
		private final String name;

		Person(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	private static class Employee {
		private final String name;

		Employee(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
