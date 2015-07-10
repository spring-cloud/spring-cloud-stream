/*
 * Copyright 2014-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.json.Jackson2JsonObjectMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageHeaderAccessor;

/**
 * Encodes requested headers into payload with format
 * {@code 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]}.
 * The 0xff indicates this new format; n is number of headers (max 255); for
 * each header, the name length (1 byte) is followed by the name, followed by
 * the value length (int) followed by the value (json).
 * <p>
 * Previously, there was no leading 0xff; the value length was 1 byte and only
 * String header values were supported (no JSON conversion).
 *
 * @author Eric Bottard
 * @author Gary Russell
 */
public class EmbeddedHeadersMessageConverter {

	private final Jackson2JsonObjectMapper objectMapper = new Jackson2JsonObjectMapper();

	public static String decodeExceptionMessage(Message<?> requestMessage) {
		return "Could not convert message: " + DatatypeConverter.printHexBinary((byte[]) requestMessage.getPayload());
	}


	/**
	 * Return a new message where some of the original headers of {@code original}
	 * have been embedded into the new message payload.
	 */
	public byte[] embedHeaders(MessageValues original, String... headers) throws Exception {
		byte[][] headerValues = new byte[headers.length][];
		int n = 0;
		int headerCount = 0;
		int headersLength = 0;
		for (String header : headers) {
			Object value = original.get(header) == null ? null
					: original.get(header);
			if (value != null) {
				String json = this.objectMapper.toJson(value);
				headerValues[n++] = json.getBytes("UTF-8");
				headerCount++;
				headersLength += header.length() + json.length();
			}
			else {
				headerValues[n++] = null;
			}
		}
		// 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
		byte[] newPayload = new byte[((byte[])original.getPayload()).length + headersLength + headerCount * 5 + 2];
		ByteBuffer byteBuffer = ByteBuffer.wrap(newPayload);
		byteBuffer.put((byte) 0xff); // signal new format
		byteBuffer.put((byte) headerCount);
		for (int i = 0; i < headers.length; i++) {
			if (headerValues[i] != null) {
				byteBuffer.put((byte) headers[i].length());
				byteBuffer.put(headers[i].getBytes("UTF-8"));
				byteBuffer.putInt(headerValues[i].length);
				byteBuffer.put(headerValues[i]);
			}
		}

		byteBuffer.put((byte[])original.getPayload());
		return byteBuffer.array();
	}

	/**
	 * Return a message where headers, that were originally embedded into the payload, have been promoted
	 * back to actual headers. The new payload is now the original payload.
	 *
	 * @param message the message to extract headers
	 * @param copyRequestHeaders boolean value to specify if original headers should be copied
	 */
	public MessageValues extractHeaders(Message<byte[]> message, boolean copyRequestHeaders) throws Exception {
		byte[] bytes = message.getPayload();
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		int headerCount = byteBuffer.get() & 0xff;
		if (headerCount < 255) {
			return oldExtractHeaders(byteBuffer, bytes, headerCount, message, copyRequestHeaders);
		}
		else {
			headerCount = byteBuffer.get() & 0xff;
			Map<String, Object> headers = new HashMap<String, Object>();
			for (int i = 0; i < headerCount; i++) {
				int len = byteBuffer.get() & 0xff;
				String headerName = new String(bytes, byteBuffer.position(), len, "UTF-8");
				byteBuffer.position(byteBuffer.position() + len);
				len = byteBuffer.getInt();
				String headerValue = new String(bytes, byteBuffer.position(), len, "UTF-8");
				Object headerContent = this.objectMapper.fromJson(headerValue, Object.class);
				headers.put(headerName, headerContent);
				byteBuffer.position(byteBuffer.position() + len);
			}
			byte[] newPayload = new byte[byteBuffer.remaining()];
			byteBuffer.get(newPayload);
			return buildMessageValues(message, newPayload, headers, copyRequestHeaders);
		}
	}

	private MessageValues oldExtractHeaders(ByteBuffer byteBuffer, byte[] bytes, int headerCount,
			Message<byte[]> message, boolean copyRequestHeaders)
			throws UnsupportedEncodingException {
		Map<String, Object> headers = new HashMap<String, Object>();
		for (int i = 0; i < headerCount; i++) {
			int len = byteBuffer.get();
			String headerName = new String(bytes, byteBuffer.position(), len, "UTF-8");
			byteBuffer.position(byteBuffer.position() + len);
			len = byteBuffer.get() & 0xff;
			String headerValue = new String(bytes, byteBuffer.position(), len, "UTF-8");
			byteBuffer.position(byteBuffer.position() + len);
			if (IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER.equals(headerName)
					|| IntegrationMessageHeaderAccessor.SEQUENCE_SIZE.equals(headerName)) {
				headers.put(headerName, Integer.parseInt(headerValue));
			}
			else {
				headers.put(headerName, headerValue);
			}
		}
		byte[] newPayload = new byte[byteBuffer.remaining()];
		byteBuffer.get(newPayload);
		return buildMessageValues(message, newPayload, headers, copyRequestHeaders);
	}

	private MessageValues buildMessageValues(Message<byte[]> message, byte[] payload, Map<String, Object> headers,
			boolean copyRequestHeaders) {
		MessageValues messageValues = new MessageValues(payload, headers);
		if (copyRequestHeaders) {
			messageValues.copyHeadersIfAbsent(message.getHeaders());
		}
		return messageValues;
	}

}
