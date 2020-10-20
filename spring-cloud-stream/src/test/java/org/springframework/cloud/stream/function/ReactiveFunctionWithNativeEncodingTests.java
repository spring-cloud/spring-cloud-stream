/*
 * Copyright 2020-2020 the original author or authors.
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
import java.util.function.Function;

import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
public class ReactiveFunctionWithNativeEncodingTests {

	@Test
	public void testFunctionIsAppliedToExistingMessageSource() throws Exception {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
			TestChannelBinderConfiguration.getCompleteConfiguration(
				FunctionsConfiguration.class)).web(WebApplicationType.NONE).run(
			"--spring.cloud.stream.function.definition=toUpperCaseReactive",
			"--spring.jmx.enabled=false",
			"--spring.cloud.stream.bindings.toUpperCaseReactive-in-0.consumer.useNativeDecoding=true",
			"--spring.cloud.stream.bindings.toUpperCaseReactive-out-0.producer.useNativeEncoding=true"
		)) {
			SimpleFunctionRegistry simpleFunctionRegistry = context.getBean(SimpleFunctionRegistry.class);

			final Field messageConverter = ReflectionUtils
				.findField(SimpleFunctionRegistry.class, "messageConverter", CompositeMessageConverter.class);
			messageConverter.setAccessible(true);
			final CompositeMessageConverter o = (CompositeMessageConverter) messageConverter.get(simpleFunctionRegistry);

			final CompositeMessageConverter spy = Mockito.spy(o);
			ReflectionUtils.setField(messageConverter, simpleFunctionRegistry, spy);

			InputDestination inputDestination = context.getBean(InputDestination.class);
			OutputDestination outputDestination = context.getBean(OutputDestination.class);
			Message<byte[]> inputMessageOne = MessageBuilder.withPayload("Hello".getBytes()).build();
			inputDestination.send(inputMessageOne);

			Message<byte[]> outputMessage = outputDestination.receive();

			assertThat(outputMessage.getPayload()).isEqualTo("HELLO".getBytes());
			// Verify that no message conversion done by the framework on the outbound.
			// If useNativeEncoding is not enabled (see the properties above),
			// then the following verification should fail.
			verify(spy, never()).toMessage(any(), any());
		}
	}

	@EnableAutoConfiguration
	public static class FunctionsConfiguration {

		@Bean
		public Function<Flux<byte[]>, Flux<byte[]>> toUpperCaseReactive() {
			return flux -> flux.map(f -> new String(f).toUpperCase().getBytes());
		}

	}
}
