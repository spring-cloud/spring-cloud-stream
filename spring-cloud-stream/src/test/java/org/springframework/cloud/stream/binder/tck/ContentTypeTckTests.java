/*
 * Copyright 2017-2018 the original author or authors.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.converter.KryoMessageConverter;
import org.springframework.cloud.stream.converter.MessageConverterUtils;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
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
	public void stringToMapStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToMapStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload())).isEqualTo("oleg");
	}

	@Test
	public void stringToMapMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToMapMessageStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload())).isEqualTo("oleg");
	}

	@Test
	// emulates 1.3 behavior
	public void stringToMapMessageStreamListenerOriginalContentType() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToMapMessageStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";

		Message<byte[]> message = MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, "text/plain")
				.setHeader("originalContentType", "application/json;charset=UTF-8")
				.build();

		source.send(message);
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload())).isEqualTo("oleg");
	}

	@Test
	public void withInternalPipeline() {
		ApplicationContext context = new SpringApplicationBuilder(InternalPipeLine.class)
				.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload())).isEqualTo("OLEG");
	}

	@Test
	public void pojoToPojo() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToPojoStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void pojoToString() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToStringStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	public void pojoToStringOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToStringStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.output.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	public void pojoToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToByteArrayStreamListener.class).web(WebApplicationType.NONE)
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
	public void pojoToByteArrayOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToByteArrayStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.output.contentType=text/plain",
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
	public void stringToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToPojoStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.input.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void typelessToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToPojoStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.input.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void typelessToPojoInboundContentTypeBindingJson() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToPojoStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.input.contentType=application/json",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void typelessMessageToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessMessageToPojoStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.bindings.input.contentType=text/plain",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void typelessMessageToPojoInboundContentTypeBindingJson() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessMessageToPojoStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.bindings.input.contentType=application/json",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void typelessToPojoWithTextHeaderContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToPojoStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeType.valueOf("text/plain"))
				.build());
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void typelessToPojoOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToMessageStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.output.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader("contentType", new MimeType("text", "plain")).build());

		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void outboundMessageWithTextContentTypeOnly() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessToMessageTextOnlyContentTypeStreamListener.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader("contentType", new MimeType("text")).build());

		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString())
				.isEqualTo("text/plain");
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void stringToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToPojoStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(),
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.TEXT_PLAIN))));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void byteArrayToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayToPojoStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.bindings.input.contentType=text/plain",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void byteArrayToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToPojoStreamListener.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(),
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.TEXT_PLAIN))));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void byteArrayToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayToByteArrayStreamListener.class).web(WebApplicationType.NONE)
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
	public void byteArrayToByteArrayInboundOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayToByteArrayStreamListener.class).web(WebApplicationType.NONE)
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
	public void pojoMessageToStringMessage() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoMessageToStringMessageStreamListener.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	public void pojoMessageToStringMessageServiceActivator() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoMessageToStringMessageServiceActivator.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	public void byteArrayMessageToStringJsonMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(
				ByteArrayMessageToStringJsonMessageStreamListener.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("{\"name\":\"bob\"}");
	}

	@Test
	public void byteArrayMessageToStringMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringMessageToStringMessageStreamListener.class)
						.web(WebApplicationType.NONE).run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("oleg");
	}

	@Test
	@SuppressWarnings("deprecation")
	public void kryo_pojoToPojo() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToPojoStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.default.contentType=application/x-java-object",
						"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);

		KryoMessageConverter converter = new KryoMessageConverter(null, true);
		@SuppressWarnings("unchecked")
		Message<byte[]> message = (Message<byte[]>) converter.toMessage(
				new Person("oleg"),
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
						MessageConverterUtils.X_JAVA_OBJECT)));

		source.send(new GenericMessage<>(message.getPayload()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage).isNotNull();
		MimeType contentType = (MimeType) outputMessage.getHeaders()
				.get(MessageHeaders.CONTENT_TYPE);
		assertThat(contentType.getSubtype()).isEqualTo("x-java-object");
		assertThat(contentType.getParameters().get("type"))
				.isEqualTo(Person.class.getName());
	}

	@Test
	@SuppressWarnings("deprecation")
	public void kryo_pojoToPojoContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToPojoStreamListener.class).web(WebApplicationType.NONE).run(
						"--spring.jmx.enabled=false",
						"--spring.cloud.stream.bindings.output.contentType=application/x-java-object");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);

		KryoMessageConverter converter = new KryoMessageConverter(null, true);
		@SuppressWarnings("unchecked")
		Message<byte[]> message = (Message<byte[]>) converter.toMessage(
				new Person("oleg"),
				new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
						MessageConverterUtils.X_JAVA_OBJECT)));

		source.send(message);
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage).isNotNull();
		MimeType contentType = (MimeType) outputMessage.getHeaders()
				.get(MessageHeaders.CONTENT_TYPE);
		assertThat(contentType.getSubtype()).isEqualTo("x-java-object");
	}

	/**
	 * This test simply demonstrates how one can override an existing MessageConverter for
	 * a given contentType. In this case we are demonstrating how Kryo converter can be
	 * overriden ('application/x-java-object' maps to Kryo).
	 */
	@Test
	public void overrideMessageConverter_defaultContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToStringStreamListener.class, CustomConverters.class)
						.web(WebApplicationType.NONE)
						.run("--spring.cloud.stream.default.contentType=application/x-java-object",
								"--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage).isNotNull();
		System.out
				.println(new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo("AlwaysStringKryoMessageConverter");
		assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeType.valueOf("application/x-java-object"));
	}

	@Test
	public void customMessageConverter_defaultContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToStringStreamListener.class, CustomConverters.class)
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
				.isEqualTo(MimeType.valueOf("foo/bar"));
	}

	// Failure tests

	@Test
	public void _jsonToPojoWrongDefaultContentTypeProperty() {
		ApplicationContext context = new SpringApplicationBuilder(
				PojoToPojoStreamListener.class).web(WebApplicationType.NONE).run(
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
	public void _toStringDefaultContentTypePropertyUnknownContentType() {
		ApplicationContext context = new SpringApplicationBuilder(
				StringToStringStreamListener.class).web(WebApplicationType.NONE).run(
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
	public void toCollectionWithParameterizedType() {
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
	public void testWithMapInputParameter() {
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
	public void testWithMapPayloadParameter() {
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
	public void testWithListInputParameter() {
		ApplicationContext context = new SpringApplicationBuilder(
				ListInputConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[\"foo\",\"bar\"]";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void testWithMessageHeadersInputParameter() {
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

	@Test
	public void testWithTypelessInputParameterAndOctetStream() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessPayloadConfiguration.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[\"foo\",\"bar\"]";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.APPLICATION_OCTET_STREAM)
				.build());
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void testWithTypelessInputParameterAndServiceActivator() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessPayloadConfigurationSA.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[\"foo\",\"bar\"]";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes()).build());
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@Test
	public void testWithTypelessMessageInputParameterAndServiceActivator() {
		ApplicationContext context = new SpringApplicationBuilder(
				TypelessMessageConfigurationSA.class).web(WebApplicationType.NONE)
						.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[\"foo\",\"bar\"]";
		source.send(MessageBuilder.withPayload(jsonPayload.getBytes()).build());
		Message<byte[]> outputMessage = target.receive();
		assertThat(new String(outputMessage.getPayload(), StandardCharsets.UTF_8))
				.isEqualTo(jsonPayload);
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class CollectionWithParameterizedTypes {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public List<Employee<Person>> echo(List<Employee<Person>> value) {
			assertThat(value.get(0) != null).isTrue();
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TextInJsonOutListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<String> echo(String value) {
			return MessageBuilder.withPayload(value).setHeader(
					MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoToPojoStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Person echo(Person value) {
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoToStringStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String echo(Person value) {
			return value.toString();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoToByteArrayStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public byte[] echo(Person value) {
			return value.toString().getBytes(StandardCharsets.UTF_8);
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ByteArrayToPojoStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Person echo(byte[] value) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(value, Person.class);
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToPojoStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Person echo(String value) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(value, Person.class);
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessToPojoStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Person echo(Object value) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			// assume it is string because CT is text/plain
			return mapper.readValue((String) value, Person.class);
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessMessageToPojoStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Person echo(Message<?> message) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			// assume it is string because CT is text/plain
			return mapper.readValue((String) message.getPayload(), Person.class);
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessToMessageStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<?> echo(Object value) throws Exception {
			return MessageBuilder.withPayload(value.toString())
					.setHeader("contentType", new MimeType("text", "plain")).build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessToMessageTextOnlyContentTypeStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<?> echo(Object value) throws Exception {
			return MessageBuilder.withPayload(value.toString())
					.setHeader("contentType", new MimeType("text")).build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ByteArrayToByteArrayStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public byte[] echo(byte[] value) {
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToStringStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String echo(String value) {
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	/*
	 * Uncomment to test MBean name quoting for ":" in bean name component of ObjectName.
	 * Commented to avoid "InstanceAlreadyExistsException" in other tests.
	 */
	// @EnableIntegrationMBeanExport
	public static class StringToMapStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String echo(@Payload Map<?, ?> value) {
			return (String) value.get("name");
		}

		@ServiceActivator(inputChannel = "input:foo.myGroup.errors")
		public void error(Message<?> message) {
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToMapMessageStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String echo(Message<Map<?, ?>> value) {
			assertThat(value.getPayload() instanceof Map).isTrue();
			return (String) value.getPayload().get("name");
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoMessageToStringMessageStreamListener {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<String> echo(Message<Person> value) {
			return MessageBuilder.withPayload(value.getPayload().toString())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoMessageToStringMessageServiceActivator {

		@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public Message<String> echo(Message<Person> value) {
			return MessageBuilder.withPayload(value.getPayload().toString())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringMessageToStringMessageStreamListener {

		@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public Message<String> echo(Message<String> value) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			Person person = mapper.readValue(value.getPayload(), Person.class);
			return MessageBuilder.withPayload(person.toString())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ByteArrayMessageToStringJsonMessageStreamListener {

		@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public Message<String> echo(Message<byte[]> value) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			Person person = mapper.readValue(value.getPayload(), Person.class);
			person.setName("bob");
			String json = mapper.writeValueAsString(person);
			return MessageBuilder.withPayload(json).build();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class InternalPipeLine {

		@StreamListener(Processor.INPUT)
		@SendTo("internalChannel")
		public String handleA(Person value) {
			return "{\"name\":\"" + value.getName().toUpperCase() + "\"}";
		}

		@Bean
		public MessageChannel internalChannel() {
			return new DirectChannel();
		}

		@StreamListener("internalChannel")
		@SendTo(Processor.OUTPUT)
		public String handleB(Person value) {
			return value.toString();
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
		@StreamMessageConverter
		public FooBarMessageConverter fooBarMessageConverter() {
			return new FooBarMessageConverter(MimeType.valueOf("foo/bar"));
		}

		@Bean
		@StreamMessageConverter
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

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class MapInputConfiguration {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Map<?, ?> echo(Map<?, ?> value) throws Exception {
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class MapPayloadConfiguration {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Map<?, ?> echo(Message<Map<?, ?>> value) throws Exception {
			return value.getPayload();
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ListInputConfiguration {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public List<?> echo(List<?> value) throws Exception {
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class MessageHeadersInputConfiguration {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Map<?, ?> echo(MessageHeaders value) throws Exception {
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessPayloadConfiguration {

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Object echo(Object value) throws Exception {
			System.out.println(value);
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessPayloadConfigurationSA {

		@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public Object echo(Object value) throws Exception {
			System.out.println(value);
			return value;
		}

	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class TypelessMessageConfigurationSA {

		@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		public Object echo(Message<?> value) throws Exception {
			System.out.println(value.getPayload());
			return value.getPayload();
		}

	}

}
