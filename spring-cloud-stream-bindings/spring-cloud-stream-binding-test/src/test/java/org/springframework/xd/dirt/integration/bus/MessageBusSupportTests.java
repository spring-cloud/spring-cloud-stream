/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.xd.dirt.integration.bus;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.xd.dirt.integration.bus.MessageBusSupport.JavaClassMimeTypeConversion;
import org.springframework.xd.dirt.integration.bus.serializer.kryo.PojoCodec;
import org.springframework.xd.tuple.DefaultTuple;
import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;
import org.springframework.xd.tuple.serializer.kryo.TupleKryoRegistrar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * @author Gary Russell
 * @author David Turanski
 */
public class MessageBusSupportTests {

	private ContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private final TestMessageBus messageBus = new TestMessageBus();

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Before
	public void setUp() {
		messageBus.setCodec(new PojoCodec(new TupleKryoRegistrar()));
	}

	@Test
	public void testBytesPassThru() {
		byte[] payload = "foo".getBytes();
		Message<byte[]> message = MessageBuilder.withPayload(payload).build();
		MessageValues converted = messageBus.serializePayloadIfNecessary(message
		);
		assertSame(payload, converted.getPayload());
		Message<?> convertedMessage = converted.toMessage();
		assertSame(payload, convertedMessage.getPayload());
		assertEquals(MimeTypeUtils.APPLICATION_OCTET_STREAM,
				contentTypeResolver.resolve(convertedMessage.getHeaders()));
		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(convertedMessage);
		payload = (byte[]) reconstructed.getPayload();
		assertSame(converted.getPayload(), payload);
		assertNull(reconstructed.get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
	}

	@Test
	public void testBytesPassThruContentType() {
		byte[] payload = "foo".getBytes();
		Message<byte[]> message = MessageBuilder.withPayload(payload)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE)
				.build();
		MessageValues messageValues = messageBus.serializePayloadIfNecessary(message
		);
		Message<?> converted = messageValues.toMessage();
		assertSame(payload, converted.getPayload());
		assertEquals(MimeTypeUtils.APPLICATION_OCTET_STREAM,
				contentTypeResolver.resolve(converted.getHeaders()));
		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		payload = (byte[]) reconstructed.getPayload();
		assertSame(converted.getPayload(), payload);
		assertEquals(MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE,
				reconstructed.get(MessageHeaders.CONTENT_TYPE));
		assertNull(reconstructed.get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
	}

	@Test
	public void testString() throws IOException {
		MessageValues convertedValues = messageBus.serializePayloadIfNecessary(
				new GenericMessage<String>("foo"));

		Message<?> converted = convertedValues.toMessage();
		assertEquals(MimeTypeUtils.TEXT_PLAIN,
				contentTypeResolver.resolve(converted.getHeaders()));
		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		assertEquals("foo", reconstructed.getPayload());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testContentTypePreserved() throws IOException {
		Message<String> inbound = MessageBuilder.withPayload("{\"foo\":\"foo\"}")
				.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON))
				.build();
		MessageValues convertedValues = messageBus.serializePayloadIfNecessary(
				inbound);

		Message<?> converted = convertedValues.toMessage();

		assertEquals(MimeTypeUtils.TEXT_PLAIN,
				contentTypeResolver.resolve(converted.getHeaders()));
		assertEquals(MimeTypeUtils.APPLICATION_JSON,
				converted.getHeaders().get(XdHeaders.XD_ORIGINAL_CONTENT_TYPE));
		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		assertEquals("{\"foo\":\"foo\"}", reconstructed.getPayload());
		assertEquals(MimeTypeUtils.APPLICATION_JSON, reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testPojoSerialization() {
		MessageValues convertedValues = messageBus.serializePayloadIfNecessary(
				new GenericMessage<Foo>(new Foo("bar"))
		);
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(Foo.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Foo) reconstructed.getPayload()).getBar());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testPojoWithXJavaObjectMimeTypeNoType() {
		MessageValues convertedValues = messageBus.serializePayloadIfNecessary(
				new GenericMessage<Foo>(new Foo("bar"))
		);
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(Foo.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Foo) reconstructed.getPayload()).getBar());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testPojoWithXJavaObjectMimeTypeExplicitType() {
		MessageValues convertedValues = messageBus.serializePayloadIfNecessary(
				new GenericMessage<Foo>(new Foo("bar"))
		);
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(Foo.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Foo) reconstructed.getPayload()).getBar());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testTupleSerialization() {
		Tuple payload = TupleBuilder.tuple().of("foo", "bar");
		MessageValues convertedValues = messageBus.serializePayloadIfNecessary(new GenericMessage<Tuple>(payload)
		);
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(DefaultTuple.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = messageBus.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Tuple) reconstructed.getPayload()).getString("foo"));
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void mimeTypeIsSimpleObject() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new Object());
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertEquals(Object.class, Class.forName(className));
	}

	@Test
	public void mimeTypeIsObjectArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new String[0]);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertEquals(String[].class, Class.forName(className));
	}

	@Test
	public void mimeTypeIsMultiDimensionalObjectArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new String[0][0][0]);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertEquals(String[][][].class, Class.forName(className));
	}

	@Test
	public void mimeTypeIsPrimitiveArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new int[0]);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertEquals(int[].class, Class.forName(className));
	}

	@Test
	public void mimeTypeIsMultiDimensionalPrimitiveArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new int[0][0][0]);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertEquals(int[][][].class, Class.forName(className));
	}

	public static class Foo {

		private String bar;

		public Foo() {
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class Bar {

		private String foo;

		public Bar() {
		}

		public Bar(String foo) {
			this.foo = foo;
		}

		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

	}

	public class TestMessageBus extends MessageBusSupport {

		@Override
		public void bindConsumer(String name, MessageChannel channel, Properties properties) {
		}

		@Override
		public void bindPubSubConsumer(String name, MessageChannel moduleInputChannel,
				Properties properties) {
		}

		@Override
		public void bindPubSubProducer(String name, MessageChannel moduleOutputChannel,
				Properties properties) {
		}

		@Override
		public void bindProducer(String name, MessageChannel channel, Properties properties) {
		}

		@Override
		public void bindRequestor(String name, MessageChannel requests, MessageChannel replies,
				Properties properties) {
		}

		@Override
		public void bindReplier(String name, MessageChannel requests, MessageChannel replies,
				Properties properties) {
		}
	}

}
