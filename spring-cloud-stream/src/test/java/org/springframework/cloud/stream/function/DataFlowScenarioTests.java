/*
 * Copyright 2021-2021 the original author or authors.
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.assertj.core.util.Arrays;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class DataFlowScenarioTests {

	private ApplicationContext context;


	private FunctionCatalog configureCatalog(Class<?>... configClass) {
		this.context = new SpringApplicationBuilder(configClass)
				.web(WebApplicationType.NONE)
				.run("--logging.level.org.springframework.cloud.function=DEBUG",
						"--spring.main.lazy-initialization=true");
		FunctionCatalog catalog = context.getBean(FunctionCatalog.class);
		return catalog;
	}

	@Test
	public void dfAppsTest() throws Exception {
		FunctionCatalog catalog = this.configureCatalog(DFConfiguration.class);

		FunctionInvocationWrapper function = catalog.lookup("byteArrayToString|splitterFunction", "text/plain");
		ProducerProperties producerProperties = new ProducerProperties();
		SpelExpressionParser parser = new SpelExpressionParser();
		Expression expression = parser.parseExpression("payload");
		producerProperties.setPartitionKeyExpression(expression);
		PartitionAwareFunctionWrapper wrapper = new PartitionAwareFunctionWrapper(function, (ConfigurableApplicationContext) context, producerProperties);
		Field f = ReflectionUtils.findField(PartitionAwareFunctionWrapper.class, "outputMessageEnricher");
		f.setAccessible(true);
		AtomicInteger i = new AtomicInteger();
		Function<Message, Message> enricher = message -> {
			i.incrementAndGet();
			assertThat(message.getPayload()).isInstanceOf(String.class);
			return message;
		};
		f.set(wrapper, enricher);
		wrapper.apply(MessageBuilder.withPayload("foo bar baz".getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain")
						.build());
		assertThat(i.get()).isEqualTo(3);
	}

	@EnableAutoConfiguration
	public static class DFConfiguration {

		@Bean
		public Function<Message<?>, Message<?>> byteArrayToString() {
			return message -> {
				if (message.getPayload() instanceof byte[]) {
					final MessageHeaders headers = message.getHeaders();
					String contentType = headers.containsKey(MessageHeaders.CONTENT_TYPE)
							? headers.get(MessageHeaders.CONTENT_TYPE).toString()
							: MimeTypeUtils.APPLICATION_JSON_VALUE;

					if (contentType.contains("text") || contentType.contains("json") || contentType.contains("x-spring-tuple")) {
						message = MessageBuilder.withPayload(new String(((byte[]) message.getPayload())))
								.copyHeaders(message.getHeaders())
								.build();
					}
				}

				return message;
			};
		}

		@Bean
		public Function<Message<?>, List<Message<?>>> splitterFunction() {
			return message -> {
				List<Object> split = Arrays.asList(((String) message.getPayload()).split(" "));
				List<Message<?>> messages = new ArrayList<>();
				for (Object value : split) {
					messages.add(MessageBuilder.withPayload(value).build());
				}
				return messages;
			};
		}
	}
}
