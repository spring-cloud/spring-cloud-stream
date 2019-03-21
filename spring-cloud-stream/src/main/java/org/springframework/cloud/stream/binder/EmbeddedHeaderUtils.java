/*
 * Copyright 2014-2017 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.springframework.integration.support.json.Jackson2JsonObjectMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.ObjectUtils;

/**
 * Encodes requested headers into payload with format
 * {@code 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]}. The 0xff indicates
 * this new format; n is number of headers (max 255); for each header, the name length (1
 * byte) is followed by the name, followed by the value length (int) followed by the value
 * (json).
 * <p>
 * Previously, there was no leading 0xff; the value length was 1 byte and only String
 * header values were supported (no JSON conversion).
 *
 * @author Eric Bottard
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @since 1.2
 */
public abstract class EmbeddedHeaderUtils {

	private static final Jackson2JsonObjectMapper objectMapper = new Jackson2JsonObjectMapper();

	public static String decodeExceptionMessage(Message<?> requestMessage) {
		return "Could not convert message: "
				+ DatatypeConverter.printHexBinary((byte[]) requestMessage.getPayload());
	}

	/**
	 * Return a new message where some of the original headers of {@code original} have
	 * been embedded into the new message payload.
	 * @param original original message
	 * @param headers headers to embedd
	 * @return a new message
	 * @throws Exception when message couldn't be generated
	 */
	public static byte[] embedHeaders(MessageValues original, String... headers)
			throws Exception {
		byte[][] headerValues = new byte[headers.length][];
		int n = 0;
		int headerCount = 0;
		int headersLength = 0;
		for (String header : headers) {
			Object value = original.get(header);
			if (value != null) {
				String json = objectMapper.toJson(value);
				headerValues[n] = json.getBytes("UTF-8");
				headerCount++;
				headersLength += header.length() + headerValues[n++].length;
			}
			else {
				headerValues[n++] = null;
			}
		}
		// 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
		byte[] newPayload = new byte[((byte[]) original.getPayload()).length
				+ headersLength + headerCount * 5 + 2];
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

		byteBuffer.put((byte[]) original.getPayload());
		return byteBuffer.array();
	}

	/**
	 * Return a message where headers, that were originally embedded into the payload,
	 * have been promoted back to actual headers. The new payload is now the original
	 * payload.
	 * @param message the message to extract headers
	 * @param copyRequestHeaders boolean value to specify if the request headers should be
	 * copied
	 * @return wrapped message values
	 * @throws Exception when extraction failed
	 */
	public static MessageValues extractHeaders(Message<byte[]> message,
			boolean copyRequestHeaders) throws Exception {
		return extractHeaders(message.getPayload(), copyRequestHeaders,
				message.getHeaders());
	}

	private static MessageValues extractHeaders(byte[] payload,
			boolean copyRequestHeaders, MessageHeaders requestHeaders) throws Exception {
		ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
		int headerCount = byteBuffer.get() & 0xff;
		if (headerCount == 0xff) {
			headerCount = byteBuffer.get() & 0xff;
			Map<String, Object> headers = new HashMap<String, Object>();
			for (int i = 0; i < headerCount; i++) {
				int len = byteBuffer.get() & 0xff;
				String headerName = new String(payload, byteBuffer.position(), len,
						"UTF-8");
				byteBuffer.position(byteBuffer.position() + len);
				len = byteBuffer.getInt();
				String headerValue = new String(payload, byteBuffer.position(), len,
						"UTF-8");
				Object headerContent = objectMapper.fromJson(headerValue, Object.class);
				headers.put(headerName, headerContent);
				byteBuffer.position(byteBuffer.position() + len);
			}
			byte[] newPayload = new byte[byteBuffer.remaining()];
			byteBuffer.get(newPayload);
			return buildMessageValues(newPayload, headers, copyRequestHeaders,
					requestHeaders);
		}
		else {
			return buildMessageValues(payload, new HashMap<>(), copyRequestHeaders,
					requestHeaders);
		}
	}

	/**
	 * Return a message where headers, that were originally embedded into the payload,
	 * have been promoted back to actual headers. The new payload is now the original
	 * payload.
	 * @param payload the message payload
	 * @return the message with extracted headers
	 * @throws Exception when extraction failed
	 */
	public static MessageValues extractHeaders(byte[] payload) throws Exception {
		return extractHeaders(payload, false, null);
	}

	private static MessageValues buildMessageValues(byte[] payload,
			Map<String, Object> headers, boolean copyRequestHeaders,
			MessageHeaders requestHeaders) {
		MessageValues messageValues = new MessageValues(payload, headers);
		if (copyRequestHeaders && requestHeaders != null) {
			messageValues.copyHeadersIfAbsent(requestHeaders);
		}
		return messageValues;
	}

	public static String[] headersToEmbed(String[] configuredHeaders) {
		String[] headersToMap;
		if (ObjectUtils.isEmpty(configuredHeaders)) {
			headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(
					BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length + configuredHeaders.length);
			System.arraycopy(configuredHeaders, 0, combinedHeadersToMap,
					BinderHeaders.STANDARD_HEADERS.length, configuredHeaders.length);
			headersToMap = combinedHeadersToMap;
		}
		return headersToMap;
	}

	/**
	 * Return true if the bytes might have embedded headers. (First byte is 0xff and long
	 * enough for at least one header).
	 * @param bytes the array.
	 * @return true if it may have embedded headers.
	 */
	public static boolean mayHaveEmbeddedHeaders(byte[] bytes) {
		return bytes.length > 8 && (bytes[0] & 0xff) == 0xff;
	}

}
