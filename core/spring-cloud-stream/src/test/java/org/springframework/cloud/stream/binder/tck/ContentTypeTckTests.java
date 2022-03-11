/*
 * Copyright 2017-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.tck;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sort of a TCK test suite to validate payload conversion is done properly by interacting
 * with binder's input/output destinations instead of its bridged channels. This means
 * that all payloads (sent/received) must be expressed in the wire format (byte[])
 *
 * @author Oleg Zhurakousky
 * @author Gary Russell
 *
 */
public class ContentTypeTckTests {

	@Test
	void stringToMapMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToMapMessageConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload())).isEqualTo("oleg");
	}

	@Test
	void pojoToPojo() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToPojoConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void pojoToString() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToStringConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	void pojoToStringOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToStringConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.echo-out-0.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	void pojoToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToByteArrayConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	void pojoToByteArrayOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToByteArrayConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	void stringToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToPojoConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.echo-on-0.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void typelessToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToPojoConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.echo-in-0.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void typelessToPojoInboundContentTypeBindingJson() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToPojoConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.echo-in-0.contentType=application/json",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void typelessMessageToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessMessageToPojoConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.bindings.echo-in-0.contentType=text/plain",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void typelessMessageToPojoInboundContentTypeBindingJson() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessMessageToPojoConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.bindings.echo-in-0.contentType=application/json",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void typelessToPojoWithTextHeaderContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToPojoConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeType.valueOf("text/plain"))
				.build());
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void typelessToPojoOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToMessageConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.echo-out-0.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader("contentType", new MimeType("text", "plain")).build());

		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void outboundMessageWithTextContentTypeOnly() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToMessageTextOnlyContentTypeConfiguration.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader("contentType", new MimeType("text")).build());

		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString())
				.startsWith("text/");
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void stringToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToPojoConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(),
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.TEXT_PLAIN))));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void byteArrayToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayToPojoConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.echo-in-0.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void byteArrayToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToPojoConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(),
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.TEXT_PLAIN))));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void byteArrayToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayToByteArrayConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void byteArrayToByteArrayInboundOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayToByteArrayConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.bindings.input.contentType=text/plain",
								"--spring.cloud.stream.bindings.output.contentType=text/plain",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void pojoMessageToStringMessage() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoMessageToStringMessageConfiguration.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	void customMessageConverter_defaultContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToStringConfiguration.class, CustomConverters.class)
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.default.contentType=foo/bar",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage).isNotNull();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("FooBarMessageConverter");
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo("foo/bar");
	}

	// Failure tests

	@Test
	void _jsonToPojoWrongDefaultContentTypeProperty() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToPojoConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.default.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		TestChannelBinder binder = context.getBean(TestChannelBinder.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		assertThat(binder.getLastError().getPayload() instanceof MessagingException)
				.isTrue();
	}

	@Test
	@Disabled
	void _toStringDefaultContentTypePropertyUnknownContentType() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToStringConfiguration.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.default.contentType=foo/bar",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		TestChannelBinder binder = context.getBean(TestChannelBinder.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		assertThat(
				binder.getLastError().getPayload() instanceof MessageConversionException)
						.isTrue();
	}

	@Test
	void toCollectionWithParameterizedType() {
		ApplicationContext context = new SpringApplicationBuilder(
				CollectionWithParameterizedTypes.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[{\"person\":{\"name\":\"jon\"},\"id\":123},{\"person\":{\"name\":\"jane\"},\"id\":456}]";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getPayload()).isEqualTo(jsonPayload.getBytes());
	}

	// ======
	@Test
	void testWithMapInputParameter() {
		ApplicationContext context = new SpringApplicationBuilder(
				MapInputConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void testWithMapPayloadParameter() {
		ApplicationContext context = new SpringApplicationBuilder(
				MapInputConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	void testWithListInputParameter() {
		ApplicationContext context = new SpringApplicationBuilder(
				ListInputConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false", "--debug");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[\"foo\",\"bar\"]";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	@Disabled // TODO fix it. We can recognize MessageHeaders and parse it out of the message properly
	void testWithMessageHeadersInputParameter() {
		ApplicationContext context = new SpringApplicationBuilder(
				MessageHeadersInputConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isNotEqualTo(jsonPayload);
		assertThat(outputMessage.getHeaders().containsKey(MessageHeaders.ID)).isTrue();
		assertThat(outputMessage.getHeaders().containsKey(MessageHeaders.CONTENT_TYPE))
				.isTrue();
	}


	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class CollectionWithParameterizedTypes {

		@Bean
		public Function<List<Employee<Person>>, List<Employee<Person>>> echo() {
			return value -> {
				assertThat(value.get(0) != null).isTrue();
				return value;
			};
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoToPojoConfiguration {

		@Bean
		public Function<Person, Person> ecgo() {
			return value -> value;
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoToStringConfiguration {

		@Bean
		public Function<Person, String> echo() {
			return value -> value.toString();
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoToByteArrayConfiguration {

		@Bean
		public Function<Person, byte[]> echo() {
			return value -> value.toString().getBytes(StandardCharsets.UTF_8);
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ByteArrayToPojoConfiguration {

		@Bean
		public Function<byte[], Person> echo(JsonMapper mapper) {
			return value -> mapper.fromJson(value, Person.class);
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToPojoConfiguration {

		@Bean
		public Function<String, Person> echo(JsonMapper mapper) {
			return value -> mapper.fromJson(value, Person.class);
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessToPojoConfiguration {

		@Bean
		public Function<Object, Person> echo(JsonMapper mapper) {
			return value -> value instanceof byte[]
					? mapper.fromJson((byte[]) value, Person.class)
							: mapper.fromJson((String) value, Person.class);
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessMessageToPojoConfiguration {

		@Bean
		public Function<Message<?>, Person> echo(JsonMapper mapper) {
			return message -> message.getPayload() instanceof byte[]
					? mapper.fromJson((byte[]) message.getPayload(), Person.class)
							: mapper.fromJson((String) message.getPayload(), Person.class);
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessToMessageConfiguration {

		@Bean
		public Function<Object, Message<?>> echo() {
			return value -> MessageBuilder.withPayload(value.toString())
					.setHeader("contentType", new MimeType("text", "plain")).build();
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessToMessageTextOnlyContentTypeConfiguration {

		@Bean
		public Function<?, Message<?>> echo() {
			return value -> {
				return MessageBuilder.withPayload(value.toString())
						.setHeader("expected-content-type", "text/*").build();
			};
		}
	}


	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ByteArrayToByteArrayConfiguration {

		@Bean
		public Function<byte[], byte[]> echo() {
			return value -> value;
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToStringConfiguration {

		@Bean
		public Function<String, String> echo() {
			return v -> v;
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToMapMessageConfiguration {

		@Bean
		public Function<Message<Map<?, ?>>, String> echo() {
			return value -> {
				assertThat(value.getPayload() instanceof Map).isTrue();
				return (String) value.getPayload().get("name");
			};
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoMessageToStringMessageConfiguration {

		@Bean
		public Function<Message<Person>, Message<String>> echo() {
			return value -> MessageBuilder.withPayload(value.getPayload().toString())
					.setHeader("expected-content-type", MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();
		}
	}

	public static class Employee<P> {

		private P person;

		private int id;

		public int getId() {
			return this.id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public P getPerson() {
			return this.person;
		}

		public void setPerson(P person) {
			this.person = person;
		}

	}

	public static class Person {

		private String name;

		public Person() {
			this(null);
		}

		public Person(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return this.name;
		}

	}

	@Configuration
	public static class CustomConverters {

		@Bean
		public FooBarMessageConverter fooBarMessageConverter() {
			return new FooBarMessageConverter(MimeType.valueOf("foo/bar"));
		}

		@Bean
		public AlwaysStringKryoMessageConverter kryoOverrideMessageConverter() {
			return new AlwaysStringKryoMessageConverter(
					MimeType.valueOf("application/x-java-object"));
		}

		/**
		 * Even though this MessageConverter has nothing to do with Kryo it still shows
		 * how Kryo conversion can be customized/overriden since it simply overriding a
		 * converter for contentType 'application/x-java-object'.
		 *
		 */
		public static class AlwaysStringKryoMessageConverter
				extends AbstractMessageConverter {

			public AlwaysStringKryoMessageConverter(MimeType supportedMimeType) {
				super(supportedMimeType);
			}

			@Override
			protected boolean supports(Class<?> clazz) {
				return clazz == null || String.class.isAssignableFrom(clazz);
			}

			@Override
			protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
					@Nullable Object conversionHint) {
				return this.getClass().getSimpleName();
			}

			@Override
			protected Object convertToInternal(Object payload,
					@Nullable MessageHeaders headers, @Nullable Object conversionHint) {
				return ((String) payload).getBytes(StandardCharsets.UTF_8);
			}

		}

		public static class FooBarMessageConverter extends AbstractMessageConverter {

			protected FooBarMessageConverter(MimeType supportedMimeType) {
				super(supportedMimeType);
			}

			@Override
			protected boolean supports(Class<?> clazz) {
				return clazz != null && String.class.isAssignableFrom(clazz);
			}

			@Override
			protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
					@Nullable Object conversionHint) {
				return this.getClass().getSimpleName();
			}

			@Override
			protected Object convertToInternal(Object payload,
					@Nullable MessageHeaders headers, @Nullable Object conversionHint) {
				return ((String) payload).getBytes(StandardCharsets.UTF_8);
			}

		}

	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class MapInputConfiguration {

		@Bean
		public Function<Map<?, ?>, Map<?, ?>> echo() {
			return value -> value;
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ListInputConfiguration {
		@Bean
		public Function<List<?>, List<?>> echo() {
			return v -> v;
		}
	}

	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class MessageHeadersInputConfiguration {

		@Bean
		public Function<MessageHeaders, Map<?, ?>> echo() {
			return v -> v;
		}
	}

}
