/*
 * Copyright 2013-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.springframework.cloud.stream.binder.AbstractBinder.JavaClassMimeTypeConversion;
import org.springframework.cloud.stream.tuple.DefaultTuple;
import org.springframework.cloud.stream.tuple.Tuple;
import org.springframework.cloud.stream.tuple.TupleBuilder;
import org.springframework.cloud.stream.tuple.integration.TupleKryoRegistrar;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;

/**
 * @author Gary Russell
 * @author David Turanski
 */
public class MessageChannelBinderSupportTests {

	private final ContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private final TestMessageChannelBinder binder = new TestMessageChannelBinder();

	@Before
	public void setUp() {
		binder.setCodec(new PojoCodec(new TupleRegistrar()));
	}

	@Test
	public void testBytesPassThru() {
		byte[] payload = "foo".getBytes();
		Message<byte[]> message = MessageBuilder.withPayload(payload).build();
		MessageValues converted = binder.serializePayloadIfNecessary(message);
		assertSame(payload, converted.getPayload());
		Message<?> convertedMessage = converted.toMessage();
		assertSame(payload, convertedMessage.getPayload());
		assertEquals(MimeTypeUtils.APPLICATION_OCTET_STREAM,
				contentTypeResolver.resolve(convertedMessage.getHeaders()));
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(convertedMessage);
		payload = (byte[]) reconstructed.getPayload();
		assertSame(converted.getPayload(), payload);
		assertNull(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
	}

	@Test
	public void testBytesPassThruContentType() {
		byte[] payload = "foo".getBytes();
		Message<byte[]> message = MessageBuilder.withPayload(payload)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE)
				.build();
		MessageValues messageValues = binder.serializePayloadIfNecessary(message);
		Message<?> converted = messageValues.toMessage();
		assertSame(payload, converted.getPayload());
		assertEquals(MimeTypeUtils.APPLICATION_OCTET_STREAM,
				contentTypeResolver.resolve(converted.getHeaders()));
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		payload = (byte[]) reconstructed.getPayload();
		assertSame(converted.getPayload(), payload);
		assertEquals(MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE,
				reconstructed.get(MessageHeaders.CONTENT_TYPE));
		assertNull(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
	}

	@Test
	public void testString() throws IOException {
		MessageValues convertedValues = binder.serializePayloadIfNecessary(
				new GenericMessage<String>("foo"));

		Message<?> converted = convertedValues.toMessage();
		assertEquals(MimeTypeUtils.TEXT_PLAIN,
				contentTypeResolver.resolve(converted.getHeaders()));
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertEquals("foo", reconstructed.getPayload());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testContentTypePreserved() throws IOException {
		Message<String> inbound = MessageBuilder.withPayload("{\"foo\":\"foo\"}")
				.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON))
				.build();
		MessageValues convertedValues = binder.serializePayloadIfNecessary(
				inbound);

		Message<?> converted = convertedValues.toMessage();

		assertEquals(MimeTypeUtils.TEXT_PLAIN,
				contentTypeResolver.resolve(converted.getHeaders()));
		assertEquals(MimeTypeUtils.APPLICATION_JSON,
				converted.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE));
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertEquals("{\"foo\":\"foo\"}", reconstructed.getPayload());
		assertEquals(MimeTypeUtils.APPLICATION_JSON, reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testPojoSerialization() {
		MessageValues convertedValues = binder.serializePayloadIfNecessary(
				new GenericMessage<Foo>(new Foo("bar")));
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(Foo.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Foo) reconstructed.getPayload()).getBar());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testPojoWithXJavaObjectMimeTypeNoType() {
		MessageValues convertedValues = binder.serializePayloadIfNecessary(
				new GenericMessage<Foo>(new Foo("bar")));
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(Foo.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Foo) reconstructed.getPayload()).getBar());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testPojoWithXJavaObjectMimeTypeExplicitType() {
		MessageValues convertedValues = binder.serializePayloadIfNecessary(
				new GenericMessage<Foo>(new Foo("bar")));
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(Foo.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertEquals("bar", ((Foo) reconstructed.getPayload()).getBar());
		assertNull(reconstructed.get(MessageHeaders.CONTENT_TYPE));
	}

	@Test
	public void testTupleSerialization() {
		Tuple payload = TupleBuilder.tuple().of("foo", "bar");
		MessageValues convertedValues = binder.serializePayloadIfNecessary(new GenericMessage<>(payload));
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertEquals("application", mimeType.getType());
		assertEquals("x-java-object", mimeType.getSubtype());
		assertEquals(DefaultTuple.class.getName(), mimeType.getParameter("type"));

		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
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

	public class TestMessageChannelBinder extends AbstractBinder<MessageChannel> {

		@Override
		protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel channel, Properties properties) {
			return null;
		}

		@Override
		public Binding<MessageChannel> bindProducer(String name, MessageChannel channel, Properties properties) {
			return null;
		}
	}

	private static class TupleRegistrar implements KryoRegistrar {
		private final TupleKryoRegistrar delegate = new TupleKryoRegistrar();

		@Override
		public void registerTypes(Kryo kryo) {
			delegate.registerTypes(kryo);
		}

		@Override
		public List<Registration> getRegistrations() {
			return delegate.getRegistrations();
		}
	}

}
