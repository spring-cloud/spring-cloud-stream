/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Sort of a TCK test suite to validate payload conversion is
 * done properly by interacting with binder's input/output destinations
 * instead of its bridged channels.
 * This means that all payloads (sent/received) must be expressed in the
 *  wire format (byte[])
 *
 * @author Oleg Zhurakousky
 *
 */
public class ContentTypeTckTests {

	@Test
	public void stringToMapStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(StringToMapStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals("oleg", new String(outputMessage.getPayload()));
	}

	@Test
	public void stringToMapMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(StringToMapMessageStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals("oleg", new String(outputMessage.getPayload()));
	}

	@Test
	// emulates 1.3 behavior
	public void stringToMapMessageStreamListenerOriginalContentType() {
		ApplicationContext context = new SpringApplicationBuilder(StringToMapMessageStreamListener.class)
				.web(WebApplicationType.NONE)
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
		assertEquals("oleg", new String(outputMessage.getPayload()));
	}


	@Test
	public void withInternalPipeline() {
		ApplicationContext context = new SpringApplicationBuilder(InternalPipeLine.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals("OLEG", new String(outputMessage.getPayload()));
	}

	@Test
	public void pojoToPojo() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void pojoToString() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToStringStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void pojoToStringOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToStringStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.output.contentType=text/plain", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.TEXT_PLAIN, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void pojoToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToByteArrayStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void pojoToByteArrayOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToByteArrayStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.output.contentType=text/plain", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.TEXT_PLAIN, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void stringToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(StringToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.input.contentType=text/plain", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void stringToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(StringToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(), new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN))));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void byteArrayToPojoInboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(ByteArrayToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.input.contentType=text/plain", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void byteArrayToPojoInboundContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(StringToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes(), new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN))));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void byteArrayToByteArray() {
		ApplicationContext context = new SpringApplicationBuilder(ByteArrayToByteArrayStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void byteArrayToByteArrayInboundOutboundContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(ByteArrayToByteArrayStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.bindings.input.contentType=text/plain", "--spring.cloud.stream.bindings.output.contentType=text/plain", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.TEXT_PLAIN, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals(jsonPayload, new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}


	@Test
	public void pojoMessageToStringMessage() {
		ApplicationContext context = new SpringApplicationBuilder(PojoMessageToStringMessageStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.TEXT_PLAIN, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void pojoMessageToStringMessageServiceActivator() {
		ApplicationContext context = new SpringApplicationBuilder(PojoMessageToStringMessageServiceActivator.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.TEXT_PLAIN, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void byteArrayMessageToStringJsonMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(ByteArrayMessageToStringJsonMessageStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.APPLICATION_JSON, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("{\"name\":\"bob\"}", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	public void byteArrayMessageToStringMessageStreamListener() {
		ApplicationContext context = new SpringApplicationBuilder(StringMessageToStringMessageStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertEquals(MimeTypeUtils.TEXT_PLAIN, outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
		assertEquals("oleg", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void kryo_pojoToPojo() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.default.contentType=application/x-java-object", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);

		KryoMessageConverter converter = new KryoMessageConverter(null, true);
		@SuppressWarnings("unchecked")
		Message<byte[]> message = (Message<byte[]>) converter
				.toMessage(new Person("oleg"), new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MessageConverterUtils.X_JAVA_OBJECT)));

		source.send(new GenericMessage<>(message.getPayload()));
		Message<byte[]> outputMessage = target.receive();
		assertNotNull(outputMessage);
		MimeType contentType = (MimeType) outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE);
		assertEquals("x-java-object", contentType.getSubtype());
		assertEquals(Person.class.getName(), contentType.getParameters().get("type"));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void kryo_pojoToPojoContentTypeHeader() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false", "--spring.cloud.stream.bindings.output.contentType=application/x-java-object");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);

		KryoMessageConverter converter = new KryoMessageConverter(null, true);
		@SuppressWarnings("unchecked")
		Message<byte[]> message = (Message<byte[]>) converter
				.toMessage(new Person("oleg"), new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MessageConverterUtils.X_JAVA_OBJECT)));

		source.send(message);
		Message<byte[]> outputMessage = target.receive();
		assertNotNull(outputMessage);
		MimeType contentType = (MimeType) outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE);
		assertEquals("x-java-object", contentType.getSubtype());
	}

	/**
	 * This test simply demonstrates how one can override an existing MessageConverter for a given contentType.
	 * In this case we are demonstrating how Kryo converter can be overriden ('application/x-java-object' maps to Kryo).
	 */
	@Test
	public void overrideMessageConverter_defaultContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(StringToStringStreamListener.class, CustomConverters.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.default.contentType=application/x-java-object", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertNotNull(outputMessage);
		System.out.println(new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
		assertEquals("AlwaysStringKryoMessageConverter", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
		assertEquals(MimeType.valueOf("application/x-java-object"), outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void customMessageConverter_defaultContentTypeBinding() {
		ApplicationContext context = new SpringApplicationBuilder(StringToStringStreamListener.class, CustomConverters.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.default.contentType=foo/bar", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertNotNull(outputMessage);
		assertEquals("FooBarMessageConverter", new String(outputMessage.getPayload(), StandardCharsets.UTF_8));
		assertEquals(MimeType.valueOf("foo/bar"), outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE));
	}

	//Failure tests

	@Test
	public void _jsonToPojoWrongDefaultContentTypeProperty() {
		ApplicationContext context = new SpringApplicationBuilder(PojoToPojoStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.default.contentType=text/plain", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		TestChannelBinder binder = context.getBean(TestChannelBinder.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		assertTrue(binder.getLastError().getPayload() instanceof MessageConversionException);
	}

	@Test
	public void _toStringDefaultContentTypePropertyUnknownContentType() {
		ApplicationContext context = new SpringApplicationBuilder(StringToStringStreamListener.class)
				.web(WebApplicationType.NONE)
				.run("--spring.cloud.stream.default.contentType=foo/bar", "--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		TestChannelBinder binder = context.getBean(TestChannelBinder.class);
		String jsonPayload = "{\"name\":\"oleg\"}";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		assertTrue(binder.getLastError().getPayload() instanceof MessageConversionException);
	}


	@Test
	public void toCollectionWithParameterizedType() {
		ApplicationContext context = new SpringApplicationBuilder(CollectionWithParameterizedTypes.class)
				.web(WebApplicationType.NONE)
				.run("--spring.jmx.enabled=false");
		InputDestination source = context.getBean(InputDestination.class);
		OutputDestination target = context.getBean(OutputDestination.class);
		String jsonPayload = "[{\"person\":{\"name\":\"jon\"},\"id\":123},{\"person\":{\"name\":\"jane\"},\"id\":456}]";
		source.send(new GenericMessage<>(jsonPayload.getBytes()));
		Message<byte[]> outputMessage = target.receive();
		assertThat(outputMessage.getPayload()).isEqualTo(jsonPayload.getBytes());
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class CollectionWithParameterizedTypes {
		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public List<Employee<Person>> echo(List<Employee<Person>> value) {
			assertTrue(value.get(0) != null);
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
			return MessageBuilder.withPayload(value).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
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
		public String echo(String value)  {
			return value;
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToMapStreamListener {
		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String echo(@Payload Map<?, ?> value)  {
			return (String) value.get("name");
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringToMapMessageStreamListener {
		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String echo(Message<Map<?, ?>> value)  {
			assertTrue(value.getPayload() instanceof Map);
			return (String) value.getPayload().get("name");
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoMessageToStringMessageStreamListener {
		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public Message<String> echo(Message<Person> value)  {
			return MessageBuilder.withPayload(value.getPayload().toString()).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class PojoMessageToStringMessageServiceActivator {
		@ServiceActivator(inputChannel=Processor.INPUT, outputChannel=Processor.OUTPUT)
		public Message<String> echo(Message<Person> value)  {
			return MessageBuilder.withPayload(value.getPayload().toString()).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class StringMessageToStringMessageStreamListener {
		@ServiceActivator(inputChannel=Processor.INPUT, outputChannel=Processor.OUTPUT)
		public Message<String> echo(Message<String> value) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			Person person = mapper.readValue(value.getPayload(), Person.class);
			return MessageBuilder.withPayload(person.toString()).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		}
	}

	@EnableBinding(Processor.class)
	@Import(TestChannelBinderConfiguration.class)
	@EnableAutoConfiguration
	public static class ByteArrayMessageToStringJsonMessageStreamListener {
		@ServiceActivator(inputChannel=Processor.INPUT, outputChannel=Processor.OUTPUT)
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
		public String handleA(Person value)  {
			return "{\"name\":\"" + value.getName().toUpperCase() + "\"}";
		}

		@Bean
		public MessageChannel internalChannel() {
			return new DirectChannel();
		}

		@StreamListener("internalChannel")
		@SendTo(Processor.OUTPUT)
		public String handleB(Person value)  {
			return value.toString();
		}
	}

	public static class Employee<P> {
		private P person;
		private int id;
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public P getPerson() {
			return person;
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
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String toString() {
			return name;
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
			return new AlwaysStringKryoMessageConverter(MimeType.valueOf("application/x-java-object"));
		}

		/**
		 * Even though this MessageConverter has nothing to do with Kryo it still shows how Kryo
		 * conversion can be customized/overriden since it simply overriding a converter for
		 * contentType 'application/x-java-object'
		 *
		 */
		public static class AlwaysStringKryoMessageConverter extends AbstractMessageConverter {
			public AlwaysStringKryoMessageConverter(MimeType supportedMimeType) {
				super(supportedMimeType);
			}

			@Override
			protected boolean supports(Class<?> clazz) {
				return clazz == null || String.class.isAssignableFrom(clazz);
			}

			protected Object convertFromInternal(
					Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
				return this.getClass().getSimpleName();
			}
			protected Object convertToInternal(
					Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {
				return ((String)payload).getBytes(StandardCharsets.UTF_8);
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

			protected Object convertFromInternal(
					Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
				return this.getClass().getSimpleName();
			}

			protected Object convertToInternal(
					Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {
				return ((String)payload).getBytes(StandardCharsets.UTF_8);
			}

		}
	}
}
