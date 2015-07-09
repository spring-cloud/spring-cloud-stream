/*
 * Copyright 2002-2015 the original author or authors.
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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Gary Russell
 * @since 1.0
 *
 */
public class MessageConverterTests {

	@Test
	public void testHeaderEmbedding() throws Exception {
		EmbeddedHeadersMessageConverter converter = new EmbeddedHeadersMessageConverter();
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar")
				.setHeader("baz", "quxx")
				.build();
		byte[] embedded = converter.embedHeaders(new MessageValues(message), "foo", "baz");
		assertEquals(0xff, embedded[0] & 0xff);
		assertEquals("\u0002\u0003foo\u0000\u0000\u0000\u0005\"bar\"\u0003baz\u0000\u0000\u0000\u0006\"quxx\"Hello",
				new String(embedded).substring(1));

		MessageValues extracted = converter.extractHeaders(MessageBuilder.withPayload(embedded).build(), false);
		assertEquals("Hello", new String((byte[])extracted.getPayload()));
		assertEquals("bar", extracted.get("foo"));
		assertEquals("quxx", extracted.get("baz"));
	}

	@Test
	public void testHeaderEmbeddingMissingHeader() throws Exception {
		EmbeddedHeadersMessageConverter converter = new EmbeddedHeadersMessageConverter();
		Message<byte[]> message = MessageBuilder.withPayload("Hello".getBytes())
				.setHeader("foo", "bar")
				.build();
		byte[] embedded = converter.embedHeaders(new MessageValues(message), "foo", "baz");
		assertEquals(0xff, embedded[0] & 0xff);
		assertEquals("\u0001\u0003foo\u0000\u0000\u0000\u0005\"bar\"Hello",
				new String(embedded).substring(1));
	}

	@Test
	public void testCanDecodeOldFormat() throws Exception {
		EmbeddedHeadersMessageConverter converter = new EmbeddedHeadersMessageConverter();
		byte[] bytes = "\u0002\u0003foo\u0003bar\u0003baz\u0004quxxHello".getBytes("UTF-8");
		Message<byte[]> message = new GenericMessage<byte[]>(bytes);
		MessageValues extracted = converter.extractHeaders(message,false);
		assertEquals("Hello", new String((byte[])extracted.getPayload()));
		assertEquals("bar", extracted.get("foo"));
		assertEquals("quxx", extracted.get("baz"));
	}

	@Test
	public void testBadDecode() throws Exception {
		EmbeddedHeadersMessageConverter converter = new EmbeddedHeadersMessageConverter();
		byte[] bytes = "\u0002\u0003foo\u0020bar\u0003baz\u0004quxxHello".getBytes("UTF-8");
		Message<byte[]> message = new GenericMessage<byte[]>(bytes);
		try {
			converter.extractHeaders(message,false);
			Assert.fail("Exception expected");
		}
		catch (Exception e) {
			String s = EmbeddedHeadersMessageConverter.decodeExceptionMessage(message);
			assertThat(e, instanceOf(StringIndexOutOfBoundsException.class));
			assertThat(s, startsWith("Could not convert message: 0203666F6F"));
		}

	}

}
