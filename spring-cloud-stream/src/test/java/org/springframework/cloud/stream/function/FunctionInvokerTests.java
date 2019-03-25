/*
 * Copyright 2018-2019 the original author or authors.
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

import java.lang.reflect.Field;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;
import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.function.pojo.Baz;
import org.springframework.cloud.stream.function.pojo.ErrorBaz;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 * @author Tolga Kavukcu
 *
 */
public class FunctionInvokerTests {

	private static String testWithFluxedConsumerValue;

	@Test
	public void testSimpleEchoConfiguration() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SimpleEchoConfiguration.class)).web(WebApplicationType.NONE).run(
								"--spring.jmx.enabled=false",
								"--spring.cloud.stream.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("{\"name\":\"bob\"}".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload())
					.isEqualTo("{\"name\":\"bob\"}".getBytes());

		}
	}

	@Test
	public void testFluxPojoFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(SimpleFluxFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("{\"name\":\"bob\"}".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Person: bob".getBytes());

		}
	}

	@Test
	public void testFluxMessagePojoFunction() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						SimpleFluxMessageFunctionConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("{\"name\":\"bob\"}".getBytes()).build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getPayload()).isEqualTo("Person: bob".getBytes());
		}
	}

	@Test
	public void testFunctionHonorsOutboundBindingContentType() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ConverterDoesNotProduceCTConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func",
										"--spring.cloud.stream.bindings.output.contentType=text/plain")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("{\"name\":\"bob\"}".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE)
					.toString()).isEqualTo("text/plain");

		}
	}

	@Test
	public void testFunctionHonorsConverterSetContentType() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(
						ConverterInjectingCTConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false",
										"--spring.cloud.stream.function.definition=func",
										"--spring.cloud.stream.bindings.output.contentType=text/plain")) {

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context
					.getBean(OutputDestination.class);

			Message<byte[]> inputMessage = MessageBuilder
					.withPayload("{\"name\":\"bob\"}".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, "foo/bar").build();
			inputDestination.send(inputMessage);

			Message<byte[]> outputMessage = outputDestination.receive();
			assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE)
					.toString()).isEqualTo("ping/pong");

		}
	}

	@Test
	public void testSameMessageTypesAreNotConverted() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(MyFunctionsConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			Message<Foo> inputMessage = new GenericMessage<>(new Foo());

			StreamFunctionProperties functionProperties = createStreamFunctionProperties();

			functionProperties.setDefinition("messageToMessageSameType");
			FunctionInvoker<Foo, Foo> messageToMessageSameType = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));
			Message<Foo> outputMessage = messageToMessageSameType
					.apply(Flux.just(inputMessage)).blockFirst();
			assertThat(inputMessage).isSameAs(outputMessage);

			functionProperties.setDefinition("pojoToPojoSameType");
			FunctionInvoker<Foo, Foo> pojoToPojoSameType = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));
			outputMessage = pojoToPojoSameType.apply(Flux.just(inputMessage))
					.blockFirst();
			assertThat(inputMessage.getPayload()).isEqualTo(outputMessage.getPayload());

			functionProperties.setDefinition("messageToMessageNoType");
			FunctionInvoker<Foo, Foo> messageToMessageNoType = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));
			outputMessage = messageToMessageNoType.apply(Flux.just(inputMessage))
					.blockFirst();
			assertThat(outputMessage).isInstanceOf(Message.class);

			functionProperties.setDefinition("withException");
			FunctionInvoker<Foo, Foo> withException = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));

			Flux<Message<Foo>> fluxOfMessages = Flux
					.just(new GenericMessage<>(new ErrorFoo()), inputMessage);
			Message<Foo> resultMessage = withException.apply(fluxOfMessages).blockFirst();
			assertThat(resultMessage.getPayload()).isNotInstanceOf(ErrorFoo.class);
		}
	}

	@Test
	public void testNativeEncodingEnabled() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(MyFunctionsConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			Message<Baz> inputMessage = new GenericMessage<>(new Baz());

			StreamFunctionProperties functionProperties = createStreamFunctionPropertiesWithNativeEncoding();

			functionProperties.setDefinition("pojoToPojoNonEmptyPojo");
			FunctionInvoker<Baz, Baz> pojoToPojoSameType = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));
			Message<Baz> outputMessage = pojoToPojoSameType.apply(Flux.just(inputMessage))
					.blockFirst();
			assertThat(inputMessage.getPayload()).isEqualTo(outputMessage.getPayload());

			Message<Baz> inputMessageWithBaz = new GenericMessage<>(new Baz());

			functionProperties.setDefinition("messageToMessageNoType");
			FunctionInvoker<Baz, Baz> messageToMessageNoType = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));
			outputMessage = messageToMessageNoType.apply(Flux.just(inputMessageWithBaz))
					.blockFirst();
			assertThat(outputMessage).isInstanceOf(Message.class);

			functionProperties.setDefinition("withExceptionNativeEncodingEnabled");
			FunctionInvoker<Baz, Baz> withException = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));

			Flux<Message<Baz>> fluxOfMessages = Flux
					.just(new GenericMessage<>(new ErrorBaz()), inputMessage);
			Message<Baz> resultMessage = withException.apply(fluxOfMessages).blockFirst();
			assertThat(resultMessage.getPayload()).isNotInstanceOf(ErrorFoo.class);
		}
	}

	@Test
	public void testWithOutNativeEncodingEnabled() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(MyFunctionsConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			Message<Baz> inputMessage = new GenericMessage<>(new Baz());

			StreamFunctionProperties functionProperties = createStreamFunctionProperties();

			functionProperties.setDefinition("pojoToPojoNonEmptyPojo");
			FunctionInvoker<Baz, Baz> pojoToPojoSameType = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));
			Message<Baz> outputMessage = pojoToPojoSameType.apply(Flux.just(inputMessage))
					.blockFirst();
			assertThat(inputMessage.getPayload())
					.isNotEqualTo(outputMessage.getPayload());

		}
	}

	@Test
	public void testWithFluxedConsumer() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(MyFunctionsConfiguration.class))
								.web(WebApplicationType.NONE)
								.run("--spring.jmx.enabled=false")) {

			String value = "Hello";
			Message<String> inputMessage = new GenericMessage<>(value);

			StreamFunctionProperties functionProperties = createStreamFunctionProperties();

			functionProperties.setDefinition("fluxConsumer");
			FunctionInvoker<String, Void> fluxedConsumer = new FunctionInvoker<>(
					functionProperties,
					context.getBean(FunctionCatalog.class),
					context.getBean(FunctionInspector.class),
					context.getBean(CompositeMessageConverterFactory.class));

			fluxedConsumer.apply(Flux.just(inputMessage)).blockFirst();

			assertThat(testWithFluxedConsumerValue).isEqualTo(value);
		}
	}

	private StreamFunctionProperties createStreamFunctionProperties() {
		StreamFunctionProperties functionProperties = new StreamFunctionProperties();
		functionProperties.setInputDestinationName("input");
		functionProperties.setOutputDestinationName("output");
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		bindingServiceProperties.getConsumerProperties("input").setMaxAttempts(3);
		try {
			Field f = ReflectionUtils.findField(StreamFunctionProperties.class,
					"bindingServiceProperties");
			f.setAccessible(true);
			f.set(functionProperties, bindingServiceProperties);
			return functionProperties;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private StreamFunctionProperties createStreamFunctionPropertiesWithNativeEncoding() {
		StreamFunctionProperties functionProperties = new StreamFunctionProperties();
		functionProperties.setInputDestinationName("input");
		functionProperties.setOutputDestinationName("output");
		BindingServiceProperties bindingServiceProperties = new BindingServiceProperties();
		bindingServiceProperties.getConsumerProperties("input").setMaxAttempts(3);
		bindingServiceProperties.getProducerProperties("output")
				.setUseNativeEncoding(true);
		try {
			Field bspField = ReflectionUtils.findField(StreamFunctionProperties.class,
					"bindingServiceProperties");
			bspField.setAccessible(true);
			bspField.set(functionProperties, bindingServiceProperties);
			return functionProperties;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class SimpleEchoConfiguration {

		@Bean
		public Function<Person, Person> func() {
			return x -> x;
		}

		public static class Person {

			private String name;

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

		}

	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class SimpleFluxFunctionConfiguration {

		@Bean
		public Function<Flux<Person>, Flux<String>> func() {
			return x -> x.map(person -> person.toString());
		}

		public static class Person {

			private String name;

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

			public String toString() {
				return "Person: " + name;
			}

		}

	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class SimpleFluxMessageFunctionConfiguration {

		@Bean
		public Function<Flux<Message<Person>>, Flux<Message<String>>> func() {
			return x -> x.map(personMessage -> {
				Person person = personMessage.getPayload();
				Message<String> message = MessageBuilder.withPayload(person.toString())
						.copyHeaders(personMessage.getHeaders()).build();
				return message;
			});
		}

		public static class Person {

			private String name;

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}

			public String toString() {
				return "Person: " + name;
			}

		}

	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class ConverterDoesNotProduceCTConfiguration {

		@Bean
		public Function<String, String> func() {
			return x -> x;
		}

		@StreamMessageConverter
		public MessageConverter customConverter() {
			return new MessageConverter() {

				@Override
				public Message<?> toMessage(Object payload, MessageHeaders headers) {
					return new GenericMessage<byte[]>(((String) payload).getBytes());
				}

				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass) {
					String contentType = (String) message.getHeaders()
							.get(MessageHeaders.CONTENT_TYPE).toString();
					if (contentType.equals("foo/bar")) {
						return new String((byte[]) message.getPayload());
					}
					return null;
				}
			};
		}

	}

	@EnableAutoConfiguration
	@EnableBinding(Processor.class)
	public static class ConverterInjectingCTConfiguration {

		@Bean
		public Function<String, String> func() {
			return x -> x;
		}

		@StreamMessageConverter
		public MessageConverter customConverter() {
			return new MessageConverter() {

				@Override
				public Message<?> toMessage(Object payload, MessageHeaders headers) {
					return MessageBuilder.withPayload(((String) payload).getBytes())
							.setHeader(MessageHeaders.CONTENT_TYPE, "ping/pong").build();
				}

				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass) {
					String contentType = (String) message.getHeaders()
							.get(MessageHeaders.CONTENT_TYPE).toString();
					if (contentType.equals("foo/bar")) {
						return new String((byte[]) message.getPayload());
					}
					return null;
				}
			};
		}

	}

	@EnableAutoConfiguration
	public static class MyFunctionsConfiguration {

		@Bean
		public Consumer<Flux<String>> fluxConsumer() {
			return f -> f.subscribe(v -> {
				System.out.println("Consuming flux: " + v);
				testWithFluxedConsumerValue = v;
			});
		}

		@Bean
		public Function<Message<Foo>, Message<Bar>> messageToMessageDifferentType() {
			return x -> MessageBuilder.withPayload(new Bar()).copyHeaders(x.getHeaders())
					.build();
		}

		@Bean
		public Function<Message<?>, Message<?>> messageToMessageAnyType() {
			return x -> MessageBuilder.withPayload(new Bar()).copyHeaders(x.getHeaders())
					.build();
		}

		@Bean
		public Function<Message<?>, Message<?>> messageToMessageNoType() {
			return x -> MessageBuilder.withPayload(new Bar()).copyHeaders(x.getHeaders())
					.build();
		}

		@Bean
		public Function<Message<Foo>, Message<Foo>> messageToMessageSameType() {
			return x -> x;
		}

		@Bean
		public Function<Foo, Foo> pojoToPojoSameType() {
			return x -> x;
		}

		@Bean
		public Function<Baz, Baz> pojoToPojoNonEmptyPojo() {
			return x -> x;
		}

		@Bean
		public Function<Foo, Foo> withException() {
			return x -> {
				if (x instanceof ErrorFoo) {
					System.out.println("Throwing exception ");
					throw new RuntimeException("Boom!");
				}
				else {
					System.out.println("All is good ");
					return x;
				}
			};
		}

		@Bean
		public Function<Baz, Baz> withExceptionNativeEncodingEnabled() {
			return x -> {
				if (x instanceof ErrorBaz) {
					System.out.println("Throwing exception ");
					throw new RuntimeException("Boom!");
				}
				else {
					System.out.println("All is good ");
					return x;
				}
			};
		}

	}

	private static class Foo {

	}

	private static class ErrorFoo extends Foo {

	}

	private static class Bar {

	}

}
