/*
 * Copyright 2002-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.nio.BufferUnderflowException;

import org.junit.jupiter.api.Test;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @since 1.0
 */
public class MessageConverterTests {

	@Test
	public void testHeaderEmbedding() throws Exception {
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar").setHeader("baz", "quxx").build();
		byte[] embedded = EmbeddedHeaderUtils.embedHeaders(new MessageValues(message),
				"foo", "baz");
		assertThat(embedded[0] & 0xff).isEqualTo(0xff);
		assertThat(new String(embedded).substring(1)).isEqualTo(
				"\u0002\u0003foo\u0000\u0000\u0000\u0005\"bar\"\u0003baz\u0000\u0000\u0000\u0006\"quxx\"Hello");

		MessageValues extracted = EmbeddedHeaderUtils
				.extractHeaders(MessageBuilder.withPayload(embedded).build(), false);
		assertThat(new String((byte[]) extracted.getPayload())).isEqualTo("Hello");
		assertThat(extracted.get("foo")).isEqualTo("bar");
		assertThat(extracted.get("baz")).isEqualTo("quxx");
	}

	@Test
	public void testConfigurableHeaders() throws Exception {
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar").setHeader("baz", "quxx")
				.setHeader("contentType", "text/plain").build();
		String[] headers = new String[] { "foo" };
		byte[] embedded = EmbeddedHeaderUtils.embedHeaders(new MessageValues(message),
				EmbeddedHeaderUtils.headersToEmbed(headers));
		assertThat(embedded[0] & 0xff).isEqualTo(0xff);
		assertThat(new String(embedded).substring(1)).isEqualTo(
				"\u0002\u000BcontentType\u0000\u0000\u0000\u000C\"text/plain\"\u0003foo\u0000\u0000\u0000\u0005\"bar\"Hello");
		MessageValues extracted = EmbeddedHeaderUtils
				.extractHeaders(MessageBuilder.withPayload(embedded).build(), false);
		assertThat(new String((byte[]) extracted.getPayload())).isEqualTo("Hello");
		assertThat(extracted.get("foo")).isEqualTo("bar");
		assertThat(extracted.get("baz")).isNull();
		assertThat(extracted.get("contentType")).isEqualTo("text/plain");
		assertThat(extracted.get("timestamp")).isNull();
		MessageValues extractedWithRequestHeaders = EmbeddedHeaderUtils
				.extractHeaders(MessageBuilder.withPayload(embedded).build(), true);
		assertThat(extractedWithRequestHeaders.get("foo")).isEqualTo("bar");
		assertThat(extractedWithRequestHeaders.get("baz")).isNull();
		assertThat(extractedWithRequestHeaders.get("contentType"))
				.isEqualTo("text/plain");
		assertThat(extractedWithRequestHeaders.get("timestamp")).isNotNull();
	}

	@Test
	public void testHeaderExtractionWithDirectPayload() throws Exception {
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar").setHeader("baz", "quxx").build();
		byte[] embedded = EmbeddedHeaderUtils.embedHeaders(new MessageValues(message),
				"foo", "baz");
		assertThat(embedded[0] & 0xff).isEqualTo(0xff);
		assertThat(new String(embedded).substring(1)).isEqualTo(
				"\u0002\u0003foo\u0000\u0000\u0000\u0005\"bar\"\u0003baz\u0000\u0000\u0000\u0006\"quxx\"Hello");

		MessageValues extracted = EmbeddedHeaderUtils.extractHeaders(embedded);
		assertThat(new String((byte[]) extracted.getPayload())).isEqualTo("Hello");
		assertThat(extracted.get("foo")).isEqualTo("bar");
		assertThat(extracted.get("baz")).isEqualTo("quxx");
	}

	@Test
	public void testUnicodeHeader() throws Exception {
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar").setHeader("baz", "ØØØØØØØØ").build();
		byte[] embedded = EmbeddedHeaderUtils.embedHeaders(new MessageValues(message),
				"foo", "baz");
		assertThat(embedded[0] & 0xff).isEqualTo(0xff);
		assertThat(new String(embedded, "UTF-8").substring(1)).isEqualTo(
				"\u0002\u0003foo\u0000\u0000\u0000\u0005\"bar\"\u0003baz\u0000\u0000\u0000\u0012\"ØØØØØØØØ\"Hello");

		MessageValues extracted = EmbeddedHeaderUtils
				.extractHeaders(MessageBuilder.withPayload(embedded).build(), false);
		assertThat(new String((byte[]) extracted.getPayload())).isEqualTo("Hello");
		assertThat(extracted.get("foo")).isEqualTo("bar");
		assertThat(extracted.get("baz")).isEqualTo("ØØØØØØØØ");
	}

	@Test
	public void testHeaderEmbeddingMissingHeader() throws Exception {
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar").build();
		byte[] embedded = EmbeddedHeaderUtils.embedHeaders(new MessageValues(message),
				"foo", "baz");
		assertThat(embedded[0] & 0xff).isEqualTo(0xff);
		assertThat(new String(embedded).substring(1))
				.isEqualTo("\u0001\u0003foo\u0000\u0000\u0000\u0005\"bar\"Hello");
	}

	@Test
	public void testBadDecode() throws Exception {
		byte[] bytes = new byte[] { (byte) 0xff, 99 };
		Message<byte[]> message = new GenericMessage<>(bytes);
		try {
			EmbeddedHeaderUtils.extractHeaders(message, false);
			fail("Exception expected");
		}
		catch (Exception e) {
			String s = EmbeddedHeaderUtils.decodeExceptionMessage(message);
			assertThat(e).isInstanceOf(BufferUnderflowException.class);
		}
	}

}
