/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.objenesis.strategy.StdInstantiatorStrategy;

import org.springframework.integration.codec.kryo.CompositeKryoRegistrar;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

/**
 * @author Vinicius Carvalho
 * @deprecated as of 2.0 all language specific type converters (kryo, java etc) are
 * deprecated and won't be supported in the future.
 */
@Deprecated
public class KryoMessageConverter implements SmartMessageConverter {

	/**
	 * Kryo mime type.
	 */
	public static final String KRYO_MIME_TYPE = "application/x-java-object";

	protected final KryoPool pool;

	private final CompositeKryoRegistrar kryoRegistrar;

	private final boolean useReferences;

	private final List<MimeType> supportedMimeTypes;

	private ConcurrentMap<String, MimeType> mimeTypesCache = new ConcurrentHashMap<>();

	private ContentTypeResolver contentTypeResolver = new DefaultContentTypeResolver();

	public KryoMessageConverter(List<KryoRegistrar> kryoRegistrars,
			boolean useReferences) {
		this.useReferences = useReferences;
		this.kryoRegistrar = CollectionUtils.isEmpty(kryoRegistrars) ? null
				: new CompositeKryoRegistrar(kryoRegistrars);
		KryoFactory factory = () -> {
			Kryo kryo = new Kryo();
			configureKryoInstance(kryo);
			return kryo;
		};
		this.pool = new KryoPool.Builder(factory).softReferences().build();
		this.supportedMimeTypes = Collections
				.singletonList(MimeType.valueOf(KRYO_MIME_TYPE));
	}

	@Nullable
	@Override
	public Object fromMessage(Message<?> message, Class<?> targetClass,
			@Nullable Object conversionHint) {
		if (!canConvertFrom(message, targetClass)) {
			return null;
		}
		if (!message.getPayload().getClass().isAssignableFrom(byte[].class)) {
			throw new MessageConversionException(
					"This converter can only convert messages with byte[] payload");
		}
		byte[] payload = (byte[]) message.getPayload();
		try {
			return deserialize(payload, targetClass);
		}
		catch (IOException e) {
			throw new MessageConversionException("Could not deserialize payload", e);
		}
	}

	@Nullable
	@Override
	public Message<?> toMessage(Object payload, @Nullable MessageHeaders headers,
			@Nullable Object conversionHint) {
		if (!canConvertTo(payload, headers)) {
			return null;
		}
		byte[] payloadToUse = serialize(payload);
		MimeType mimeType = getDefaultContentType(payload);
		if (headers != null) {
			MessageHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(headers,
					MessageHeaderAccessor.class);
			if (accessor != null && accessor.isMutable()) {
				if (mimeType != null) {
					accessor.setHeader(MessageHeaders.CONTENT_TYPE, mimeType);
				}
				return MessageBuilder.createMessage(payloadToUse,
						accessor.getMessageHeaders());
			}
		}
		MessageBuilder<?> builder = MessageBuilder.withPayload(payloadToUse);
		if (headers != null) {
			builder.copyHeaders(headers);
		}
		if (mimeType != null) {
			builder.setHeader(MessageHeaders.CONTENT_TYPE, mimeType);
		}
		return builder.build();
	}

	private boolean canConvertTo(Object payload, MessageHeaders headers) {
		return (supports(payload.getClass()) && supportsMimeType(headers));
	}

	@Nullable
	protected MimeType getDefaultContentType(Object payload) {
		return mimeTypeFromObject(payload);
	}

	protected byte[] serialize(Object payload) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final Output output = new Output(baos);
		this.pool.run(kryo -> {
			kryo.writeObject(output, payload);
			return Void.TYPE;
		});
		output.close();
		return baos.toByteArray();
	}

	protected <T> T deserialize(byte[] bytes, Class<T> type) throws IOException {
		Assert.notNull(bytes, "'bytes' cannot be null");
		final Input input = new Input(bytes);
		try {
			return deserialize(input, type);
		}
		finally {
			input.close();
		}
	}

	protected <T> T deserialize(InputStream inputStream, final Class<T> type)
			throws IOException {
		Assert.notNull(inputStream, "'inputStream' cannot be null");
		Assert.notNull(type, "'type' cannot be null");
		final Input input = (inputStream instanceof Input ? (Input) inputStream
				: new Input(inputStream));
		T result = null;
		try {
			result = this.pool.run(kryo -> kryo.readObject(input, type));
		}
		finally {
			input.close();
		}
		return result;
	}

	protected void configureKryoInstance(Kryo kryo) {
		kryo.setInstantiatorStrategy(
				new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
		if (this.kryoRegistrar != null) {
			this.kryoRegistrar.registerTypes(kryo);
		}
		kryo.setReferences(this.useReferences);
	}

	protected MimeType mimeTypeFromObject(Object payload) {
		Assert.notNull(payload, "payload object cannot be null.");
		String className = payload.getClass().getName();
		MimeType mimeType = this.mimeTypesCache.get(className);
		if (mimeType == null) {
			String modifiedClassName = className;
			if (payload.getClass().isArray()) {
				// Need to remove trailing ';' for an object array, e.g.
				// "[Ljava.lang.String;" or multi-dimensional
				// "[[[Ljava.lang.String;"
				if (modifiedClassName.endsWith(";")) {
					modifiedClassName = modifiedClassName.substring(0,
							modifiedClassName.length() - 1);
				}
				// Wrap in quotes to handle the illegal '[' character
				modifiedClassName = "\"" + modifiedClassName + "\"";
			}
			mimeType = MimeType.valueOf(KRYO_MIME_TYPE + ";type=" + modifiedClassName);
			this.mimeTypesCache.put(className, mimeType);
		}
		return mimeType;
	}

	protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
		return (supports(targetClass) && supportsMimeType(message.getHeaders()));
	}

	protected boolean supportsMimeType(@Nullable MessageHeaders headers) {
		if (getSupportedMimeTypes().isEmpty()) {
			return true;
		}
		MimeType mimeType = getMimeType(headers);
		if (mimeType == null) {
			return false;
		}
		for (MimeType current : getSupportedMimeTypes()) {
			if (current.getType().equals(mimeType.getType())
					&& current.getSubtype().equals(mimeType.getSubtype())) {
				return true;
			}
		}
		return false;
	}

	@Nullable
	protected MimeType getMimeType(@Nullable MessageHeaders headers) {
		return (headers != null && this.contentTypeResolver != null
				? this.contentTypeResolver.resolve(headers) : null);
	}

	private boolean supports(Class<?> targetClass) {
		return true;
	}

	@Nullable
	@Override
	public Object fromMessage(Message<?> message, Class<?> targetClass) {
		return fromMessage(message, targetClass, null);
	}

	@Nullable
	@Override
	public Message<?> toMessage(Object payload, @Nullable MessageHeaders headers) {
		return toMessage(payload, headers, null);
	}

	public List<MimeType> getSupportedMimeTypes() {
		return this.supportedMimeTypes;
	}

}
