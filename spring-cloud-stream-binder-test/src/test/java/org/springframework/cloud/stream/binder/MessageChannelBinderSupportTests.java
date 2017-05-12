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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import org.junit.Before;
import org.junit.Test;

import org.springframework.cloud.stream.binder.AbstractBinder.JavaClassMimeTypeConversion;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.tuple.TupleKryoRegistrar;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.tuple.DefaultTuple;
import org.springframework.tuple.Tuple;
import org.springframework.tuple.TupleBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
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
		assertThat(converted.getPayload()).isSameAs(payload);
		Message<?> convertedMessage = converted.toMessage();
		assertThat(convertedMessage.getPayload()).isSameAs(payload);
		assertThat(contentTypeResolver.resolve(convertedMessage.getHeaders()))
				.isEqualTo(MimeTypeUtils.APPLICATION_OCTET_STREAM);
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(convertedMessage);
		payload = (byte[]) reconstructed.getPayload();
		assertThat(converted.getPayload()).isSameAs(payload);
		assertThat(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
	}

	@Test
	public void testBytesPassThruContentType() {
		byte[] payload = "foo".getBytes();
		Message<byte[]> message = MessageBuilder.withPayload(payload)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE).build();
		MessageValues messageValues = binder.serializePayloadIfNecessary(message);
		Message<?> converted = messageValues.toMessage();
		assertThat(converted.getPayload()).isSameAs(payload);
		assertThat(contentTypeResolver.resolve(converted.getHeaders()))
				.isEqualTo(MimeTypeUtils.APPLICATION_OCTET_STREAM);
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		payload = (byte[]) reconstructed.getPayload();
		assertThat(converted.getPayload()).isSameAs(payload);
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE);
		assertThat(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
	}

	@Test
	public void testString() throws IOException {
		MessageValues convertedValues = binder.serializePayloadIfNecessary(new GenericMessage<>("foo"));
		Message<?> converted = convertedValues.toMessage();
		assertThat(contentTypeResolver.resolve(converted.getHeaders())).isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertThat(reconstructed.getPayload()).isEqualTo("foo");
		assertThat(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
	}

	@Test
	public void testStringXML() throws IOException {
		Message<?> message = MessageBuilder
				.withPayload("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><test></test>")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_XML).build();
		Message<?> converted = binder.serializePayloadIfNecessary(message).toMessage();
		assertThat(contentTypeResolver.resolve(converted.getHeaders())).isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertThat(reconstructed.getPayload())
				.isEqualTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><test></test>");
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MimeTypeUtils.TEXT_XML.toString());
	}

	@Test
	public void testContentTypePreservedForJson() throws IOException {
		Message<String> inbound = MessageBuilder.withPayload("{\"foo\":\"foo\"}")
				.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON))
				.build();
		MessageValues convertedValues = binder.serializePayloadIfNecessary(inbound);
		Message<?> converted = convertedValues.toMessage();
		assertThat(contentTypeResolver.resolve(converted.getHeaders())).isEqualTo(MimeTypeUtils.APPLICATION_JSON);
		assertThat(converted.getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertThat(reconstructed.getPayload()).isEqualTo("{\"foo\":\"foo\"}");
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MimeTypeUtils.APPLICATION_JSON_VALUE);
	}

	@Test
	public void testContentTypePreservedForNonSCStApp() {
		Message<String> inbound = MessageBuilder.withPayload("{\"foo\":\"bar\"}")
				.copyHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON))
				.build();
		MessageValues reconstructed = binder.deserializePayloadIfNecessary(inbound);
		assertThat(reconstructed.getPayload()).isEqualTo("{\"foo\":\"bar\"}");
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MimeTypeUtils.APPLICATION_JSON);
	}

	@Test
	public void testPojoSerialization() {
		MessageValues convertedValues = binder.serializePayloadIfNecessary(new GenericMessage<>(new Foo("bar")));
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertThat(mimeType.getType()).isEqualTo("application");
		assertThat(mimeType.getSubtype()).isEqualTo("x-java-object");
		assertThat(mimeType.getParameter("type")).isEqualTo(Foo.class.getName());

		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertThat(((Foo) reconstructed.getPayload()).getBar()).isEqualTo("bar");
		assertThat(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(
				"application/x-java-object;type=org.springframework.cloud.stream.binder.MessageChannelBinderSupportTests$Foo");
	}

	@Test
	public void testTupleSerialization() {
		Tuple payload = TupleBuilder.tuple().of("foo", "bar");
		MessageValues convertedValues = binder.serializePayloadIfNecessary(new GenericMessage<>(payload));
		Message<?> converted = convertedValues.toMessage();
		MimeType mimeType = contentTypeResolver.resolve(converted.getHeaders());
		assertThat(mimeType.getType()).isEqualTo("application");
		assertThat(mimeType.getSubtype()).isEqualTo("x-java-object");
		assertThat(mimeType.getParameter("type")).isEqualTo(DefaultTuple.class.getName());

		MessageValues reconstructed = binder.deserializePayloadIfNecessary(converted);
		assertThat(((Tuple) reconstructed.getPayload()).getString("foo")).isEqualTo("bar");
		assertThat(reconstructed.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		assertThat(reconstructed.get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo("application/x-java-object;type=org.springframework.tuple.DefaultTuple");
	}

	@Test
	public void mimeTypeIsSimpleObject() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new Object(), null);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertThat(Class.forName(className)).isEqualTo(Object.class);
	}

	@Test
	public void mimeTypeIsObjectArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new String[0], null);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertThat(Class.forName(className)).isEqualTo(String[].class);
	}

	@Test
	public void mimeTypeIsMultiDimensionalObjectArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new String[0][0][0], null);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertThat(Class.forName(className)).isEqualTo(String[][][].class);
	}

	@Test
	public void mimeTypeIsPrimitiveArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new int[0], null);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertThat(Class.forName(className)).isEqualTo(int[].class);
	}

	@Test
	public void mimeTypeIsMultiDimensionalPrimitiveArray() throws ClassNotFoundException {
		MimeType mt = JavaClassMimeTypeConversion.mimeTypeFromObject(new int[0][0][0], null);
		String className = JavaClassMimeTypeConversion.classNameFromMimeType(mt);
		assertThat(Class.forName(className)).isEqualTo(int[][][].class);
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

	public class TestMessageChannelBinder
			extends AbstractBinder<MessageChannel, ConsumerProperties, ProducerProperties> {

		@Override
		protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel channel,
				ConsumerProperties properties) {
			return null;
		}

		@Override
		public Binding<MessageChannel> doBindProducer(String name, MessageChannel channel,
				ProducerProperties properties) {
			return null;
		}
	}

}
