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

import java.util.function.Function;

import org.junit.Test;

import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class FunctionInvokerTests {

	@Test
	public void testSameMessageTypesAreNotConverted() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration.getCompleteConfiguration(MyFunctionsConfiguration.class)).web(
				WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false")) {

			Message<Foo> inputMessage = new GenericMessage<>(new Foo());

			FunctionInvoker<Foo, Foo> messageToMessageSameType = new FunctionInvoker<>("messageToMessageSameType",
					new FunctionCatalogWrapper(context.getBean(FunctionCatalog.class)), context.getBean(FunctionInspector.class), context.getBean(CompositeMessageConverterFactory.class));
			Message<Foo> outputMessage = messageToMessageSameType.apply(Flux.just(inputMessage)).blockFirst();
			assertThat(inputMessage).isSameAs(outputMessage);

			FunctionInvoker<Foo, Foo> pojoToPojoSameType = new FunctionInvoker<>("pojoToPojoSameType",
					new FunctionCatalogWrapper(context.getBean(FunctionCatalog.class)), context.getBean(FunctionInspector.class), context.getBean(CompositeMessageConverterFactory.class));
			outputMessage = pojoToPojoSameType.apply(Flux.just(inputMessage)).blockFirst();
			assertThat(inputMessage.getPayload()).isEqualTo(outputMessage.getPayload());

			FunctionInvoker<Foo, Foo> messageToMessageNoType = new FunctionInvoker<>("messageToMessageNoType",
					new FunctionCatalogWrapper(context.getBean(FunctionCatalog.class)), context.getBean(FunctionInspector.class), context.getBean(CompositeMessageConverterFactory.class));
			outputMessage = messageToMessageNoType.apply(Flux.just(inputMessage)).blockFirst();
			assertThat(outputMessage).isInstanceOf(Message.class);
		}
	}

	@EnableAutoConfiguration
	public static class MyFunctionsConfiguration {

		@Bean
		public Function<Message<Foo>, Message<Bar>> messageToMessageDifferentType() {
			return x -> MessageBuilder.withPayload(new Bar()).copyHeaders(x.getHeaders()).build();
		}

		@Bean
		public Function<Message<?>, Message<?>> messageToMessageAnyType() {
			return x -> MessageBuilder.withPayload(new Bar()).copyHeaders(x.getHeaders()).build();
		}

		@Bean
		public Function<Message<?>, Message<?>> messageToMessageNoType() {
			return x -> MessageBuilder.withPayload(new Bar()).copyHeaders(x.getHeaders()).build();
		}

		@Bean
		public Function<Message<Foo>, Message<Foo>> messageToMessageSameType() {
			return x -> x;
		}

		@Bean
		public Function<Foo, Foo> pojoToPojoSameType() {
			return x -> x;
		}

	}

	private static class Foo {

	}

	private static class Bar {

	}
}
