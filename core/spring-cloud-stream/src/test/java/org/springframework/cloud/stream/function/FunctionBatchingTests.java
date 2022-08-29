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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Gary Russel
 * @author Oleg Zhurakousky
 * @author David Turanski
 *
 * @since 3.0
 */
public class FunctionBatchingTests {

	@Test
	public void testMessageBatchConfiguration() {
		TestChannelBinderConfiguration.applicationContextRunner(MessageBatchConfiguration.class)
				.withPropertyValues("spring.jmx.enabled=false",
						"spring.cloud.stream.function.definition=func2",
						"spring.cloud.stream.bindings.input.consumer.batch-mode=true")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context
							.getBean(OutputDestination.class);

					List<byte[]> list = new ArrayList<>();
					list.add("{\"name\":\"bob\"}".getBytes());
					list.add("{\"name\":\"jill\"}".getBytes());
					Message<List<byte[]>> inputMessage = MessageBuilder
							.withPayload(list)
							.build();
					inputDestination.send(inputMessage);

					Message<byte[]> outputMessage = outputDestination.receive();
					assertThat(outputMessage).isNotNull();
					assertThat(outputMessage.getPayload())
							.isEqualTo("{\"name\":\"bob\"}".getBytes());

					context.stop();
				});
	}

	@Test
	public void testListPayloadConfiguration() {
		TestChannelBinderConfiguration.applicationContextRunner(ListPayloadNotBatchConfiguration.class)
				.withPropertyValues("spring.jmx.enabled=false",
						"spring.cloud.stream.function.definition=func")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context
							.getBean(OutputDestination.class);

					Message<byte[]> inputMessage = MessageBuilder
							.withPayload("[{\"name\":\"bob\"},{\"name\":\"jill\"}]".getBytes())
							.build();
					inputDestination.send(inputMessage);

					Message<byte[]> outputMessage = outputDestination.receive();
					assertThat(outputMessage).isNotNull();
					assertThat(outputMessage.getPayload())
							.isEqualTo("{\"name\":\"bob\"}".getBytes());

					context.stop();
				});
	}

	@Test
	public void testListStringPayloadConfigurationTextPlain() {
		TestChannelBinderConfiguration.applicationContextRunner(ListStringPayloadConfiguration.class)
				.withPropertyValues("spring.jmx.enabled=false",
						"spring.cloud.stream.function.definition=func",
						"spring.cloud.stream.bindings.func-in-0.content-type=text/plain")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context
							.getBean(OutputDestination.class);

					List bytes = Arrays.asList(new Object[] {"abc".getBytes(), "xyz".getBytes()});
					Message inputMessage = MessageBuilder.withPayload(bytes).build();
					inputDestination.send(inputMessage);

					Message<byte[]> outputMessage = outputDestination.receive();
					assertThat(new String(outputMessage.getPayload())).isEqualTo("[abc, xyz]");
					context.stop();
				});
	}

	@Test
	public void testListObjectPayloadObjectConfigurationTextPlain() {
		TestChannelBinderConfiguration.applicationContextRunner(ListObjectPayloadConfiguration.class)
				.withPropertyValues("spring.jmx.enabled=false",
						"spring.cloud.stream.function.definition=func",
						"spring.cloud.stream.bindings.func-in-0.content-type=text/plain")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context
							.getBean(OutputDestination.class);

					List bytes = Arrays.asList(new Object[] {"abc".getBytes(), "xyz".getBytes()});
					Message inputMessage = MessageBuilder.withPayload(bytes).build();
					inputDestination.send(inputMessage);

					Message<byte[]> outputMessage = outputDestination.receive();
					assertThat(new String(outputMessage.getPayload())).isEqualTo("[abc, xyz]");
					context.stop();
				});
	}

	@Test
	public void testSimpleBatchConfiguration() {
		TestChannelBinderConfiguration.applicationContextRunner(SimpleBatchConfiguration.class)
				.withPropertyValues(
						"spring.jmx.enabled=false",
						"spring.cloud.stream.function.definition=func",
						"spring.cloud.stream.bindings.input.consumer.batch-mode=true")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context
							.getBean(OutputDestination.class);

					List<byte[]> list = new ArrayList<>();
					list.add("{\"name\":\"bob\"}".getBytes());
					list.add("{\"name\":\"jill\"}".getBytes());
					Message<List<byte[]>> inputMessage = MessageBuilder
							.withPayload(list)
							.build();
					inputDestination.send(inputMessage);

					Message<byte[]> outputMessage = outputDestination.receive();
					assertThat(outputMessage).isNotNull();
					assertThat(outputMessage.getPayload())
							.isEqualTo("{\"name\":\"bob\"}".getBytes());
					context.stop();
				});
	}

	@Test
	public void testNestedBatchConfiguration() {
		TestChannelBinderConfiguration.applicationContextRunner(NestedBatchConfiguration.class)
				.withPropertyValues("spring.jmx.enabled=false",
						"spring.cloud.stream.function.definition=func",
						"spring.cloud.stream.bindings.input.consumer.batch-mode=true")
				.run(context -> {
					InputDestination inputDestination = context.getBean(InputDestination.class);
					OutputDestination outputDestination = context
							.getBean(OutputDestination.class);

					List<byte[]> list = new ArrayList<>();
					list.add("[{\"name\":\"bob\"},{\"name\":\"jill\"}]".getBytes());
					Message<List<byte[]>> inputMessage = MessageBuilder
							.withPayload(list)
							.build();
					inputDestination.send(inputMessage);

					Message<byte[]> outputMessage = outputDestination.receive();
					assertThat(outputMessage).isNotNull();
					assertThat(outputMessage.getPayload())
							.isEqualTo("{\"name\":\"bob\"}".getBytes());
					context.stop();
				});
	}

	@EnableAutoConfiguration
	public static class SimpleBatchConfiguration {

		@Bean
		public Function<List<Person>, Person> func() {
			return x -> x.get(0);
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
	public static class ListStringPayloadConfiguration {
		@Bean
		public Function<List<String>, String> func() {
			return x -> x.toString();
		}
	}

	@EnableAutoConfiguration
	public static class ListObjectPayloadConfiguration {
		@Bean
		public Function<List<Object>, String> func() {
			return x -> x.toString();
		}
	}

	@EnableAutoConfiguration
	public static class ListPayloadNotBatchConfiguration {

		@Bean
		public Function<List<Person>, Person> func() {
			return x -> x.get(0);
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
	public static class NestedBatchConfiguration {

		@Bean
		public Function<List<List<Person>>, Person> func() {
			return x -> x.get(0).get(0);
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
	public static class MessageBatchConfiguration {

		@Bean
		public Function<Message<List<Person>>, Person> func() {
			return x -> {
				Object o = x.getPayload().get(2);
				assertThat(o).isNull();
				return (Person) x.getPayload().get(0);
			};
		}

		@Bean
		public Function<Message<List<Person>>, Person> func2() {
			return x -> x.getPayload().get(0);
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

}
