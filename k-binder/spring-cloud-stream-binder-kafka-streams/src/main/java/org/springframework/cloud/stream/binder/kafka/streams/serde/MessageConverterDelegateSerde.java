/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.serde;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * A {@link Serde} implementation that wraps the list of {@link MessageConverter}s from
 * {@link CompositeMessageConverter}.
 *
 * The primary motivation for this class is to provide an avro based {@link Serde} that is
 * compatible with the schema registry that Spring Cloud Stream provides. When using the
 * schema registry support from Spring Cloud Stream in a Kafka Streams binder based
 * application, the applications can deserialize the incoming Kafka Streams records using
 * the built in Avro {@link MessageConverter}. However, this same message conversion
 * approach will not work downstream in other operations in the topology for Kafka Streams
 * as some of them needs a {@link Serde} instance that can talk to the Spring Cloud Stream
 * provided Schema Registry. This implementation will solve that problem.
 *
 * Only Avro and JSON based converters are exposed as binder provided {@link Serde}
 * implementations currently.
 *
 * Users of this class must call the
 * {@link MessageConverterDelegateSerde#configure(Map, boolean)} method to configure the
 * {@link Serde} object. At the very least the configuration map must include a key called
 * "valueClass" to indicate the type of the target object for deserialization. If any
 * other content type other than JSON is needed (only Avro is available now other than
 * JSON), that needs to be included in the configuration map with the key "contentType".
 * For example,
 *
 * <pre class="code">
 * Map&lt;String, Object&gt; config = new HashMap&lt;&gt;();
 * config.put("valueClass", Foo.class);
 * config.put("contentType", "application/avro");
 * </pre>
 *
 * Then use the above map when calling the configure method.
 *
 * This class is only intended to be used when writing a Spring Cloud Stream Kafka Streams
 * application that uses Spring Cloud Stream schema registry for schema evolution.
 *
 * An instance of this class is provided as a bean by the binder configuration and
 * typically the applications can autowire that bean. This is the expected usage pattern
 * of this class.
 *
 * @param <T> type of the object to marshall
 * @author Soby Chacko
 * @since 3.0
 * @deprecated in favor of other schema registry providers instead of Spring Cloud Schema Registry. See its motivation above.
 */
@Deprecated
public class MessageConverterDelegateSerde<T> implements Serde<T> {

	private static final String VALUE_CLASS_HEADER = "valueClass";

	private static final String AVRO_FORMAT = "avro";

	private static final MimeType DEFAULT_AVRO_MIME_TYPE = new MimeType("application",
			"*+" + AVRO_FORMAT);

	private final MessageConverterDelegateDeserializer<T> messageConverterDelegateDeserializer;

	private final MessageConverterDelegateSerializer<T> messageConverterDelegateSerializer;

	public MessageConverterDelegateSerde(
			CompositeMessageConverter compositeMessageConverter) {
		this.messageConverterDelegateDeserializer = new MessageConverterDelegateDeserializer<>(
				compositeMessageConverter);
		this.messageConverterDelegateSerializer = new MessageConverterDelegateSerializer<>(
				compositeMessageConverter);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.messageConverterDelegateDeserializer.configure(configs, isKey);
		this.messageConverterDelegateSerializer.configure(configs, isKey);
	}

	@Override
	public void close() {
		// No-op
	}

	@Override
	public Serializer<T> serializer() {
		return this.messageConverterDelegateSerializer;
	}

	@Override
	public Deserializer<T> deserializer() {
		return this.messageConverterDelegateDeserializer;
	}

	private static MimeType resolveMimeType(Map<String, ?> configs) {
		if (configs.containsKey(MessageHeaders.CONTENT_TYPE)) {
			String contentType = (String) configs.get(MessageHeaders.CONTENT_TYPE);
			if (DEFAULT_AVRO_MIME_TYPE.equals(MimeTypeUtils.parseMimeType(contentType))) {
				return DEFAULT_AVRO_MIME_TYPE;
			}
			else if (contentType.contains("avro")) {
				return MimeTypeUtils.parseMimeType("application/avro");
			}
			else {
				return new MimeType("application", "json", StandardCharsets.UTF_8);
			}
		}
		else {
			return new MimeType("application", "json", StandardCharsets.UTF_8);
		}
	}

	/**
	 * Custom {@link Deserializer} that uses the {@link org.springframework.cloud.stream.converter.CompositeMessageConverterFactory}.
	 *
	 * @param <U> parameterized target type for deserialization
	 */
	private static class MessageConverterDelegateDeserializer<U> implements Deserializer<U> {

		private final MessageConverter messageConverter;

		private MimeType mimeType;

		private Class<?> valueClass;

		MessageConverterDelegateDeserializer(
				CompositeMessageConverter compositeMessageConverter) {
			this.messageConverter = compositeMessageConverter;
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			Assert.isTrue(configs.containsKey(VALUE_CLASS_HEADER),
					"Deserializers must provide a configuration for valueClass.");
			final Object valueClass = configs.get(VALUE_CLASS_HEADER);
			Assert.isTrue(valueClass instanceof Class,
					"Deserializers must provide a valid value for valueClass.");
			this.valueClass = (Class<?>) valueClass;
			this.mimeType = resolveMimeType(configs);
		}

		@SuppressWarnings("unchecked")
		@Override
		public U deserialize(String topic, byte[] data) {
			Message<?> message = MessageBuilder.withPayload(data)
					.setHeader(MessageHeaders.CONTENT_TYPE, this.mimeType.toString())
					.build();
			U messageConverted = (U) this.messageConverter.fromMessage(message,
					this.valueClass);
			Assert.notNull(messageConverted, "Deserialization failed.");
			return messageConverted;
		}

		@Override
		public void close() {
			// No-op
		}

	}

	/**
	 * Custom {@link Serializer} that uses the {@link org.springframework.cloud.stream.converter.CompositeMessageConverterFactory}.
	 *
	 * @param <V> parameterized type for serialization
	 */
	private static class MessageConverterDelegateSerializer<V> implements Serializer<V> {

		private final MessageConverter messageConverter;

		private MimeType mimeType;

		MessageConverterDelegateSerializer(
				CompositeMessageConverter compositeMessageConverter) {
			this.messageConverter = compositeMessageConverter;
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			this.mimeType = resolveMimeType(configs);
		}

		@Override
		public byte[] serialize(String topic, V data) {
			Message<?> message = MessageBuilder.withPayload(data).build();
			Map<String, Object> headers = new HashMap<>(message.getHeaders());
			headers.put(MessageHeaders.CONTENT_TYPE, this.mimeType.toString());
			MessageHeaders messageHeaders = new MessageHeaders(headers);
			final Object payload = this.messageConverter
					.toMessage(message.getPayload(), messageHeaders).getPayload();
			return (byte[]) payload;
		}

		@Override
		public void close() {
			// No-op
		}

	}

}
