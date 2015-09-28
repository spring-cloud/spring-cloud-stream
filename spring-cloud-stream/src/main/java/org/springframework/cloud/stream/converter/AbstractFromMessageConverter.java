/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;


/**
 * Base class for converters applied via Spring Integration 4.x data type channels.
 *
 * Extend this class to implement {@link org.springframework.messaging.converter.MessageConverter MessageConverters}
 * used with custom Message conversion. Only {@link #fromMessage} is supported.
 *
 * @author David Turanski
 */
public abstract class AbstractFromMessageConverter extends AbstractMessageConverter {

	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final List<MimeType> targetMimeTypes;

	/**
	 * Creates a converter that ignores content-type message headers
	 *
	 * @param targetMimeType the required target type
	 */
	protected AbstractFromMessageConverter(MimeType targetMimeType) {
		this(new ArrayList<MimeType>(), targetMimeType, new StrictContentTypeResolver(targetMimeType));
	}

	protected AbstractFromMessageConverter(Collection<MimeType> targetMimeTypes) {
		this(new ArrayList<MimeType>(), targetMimeTypes, new StringConvertingContentTypeResolver());
	}

	/**
	 * Creates a converter that handles one or more content-type message headers
	 *
	 * @param supportedSourceMimeTypes list of {@link MimeType} that may present in content-type header
	 * @param targetMimeType the required target type
	 */
	protected AbstractFromMessageConverter(Collection<MimeType> supportedSourceMimeTypes, MimeType targetMimeType,
			ContentTypeResolver contentTypeResolver) {
		super(supportedSourceMimeTypes);
		Assert.notNull(targetMimeType, "'targetMimeType' cannot be null");
		setContentTypeResolver(contentTypeResolver);
		this.targetMimeTypes = Collections.singletonList(targetMimeType);
	}

	/**
	 * Creates a converter that handles one or more content-type message headers and one or more target MIME types
	 *
	 * @param supportedSourceMimeTypes a list of supported content types
	 * @param targetMimeTypes a list of supported target types
	 * @param contentTypeResolver the {@link ContentTypeResolver} to use
	 */
	protected AbstractFromMessageConverter(Collection<MimeType> supportedSourceMimeTypes,
			Collection<MimeType> targetMimeTypes,
			ContentTypeResolver contentTypeResolver) {
		super(supportedSourceMimeTypes);
		Assert.notNull(targetMimeTypes, "'targetMimeTypes' cannot be null");
		setContentTypeResolver(contentTypeResolver);
		this.targetMimeTypes = new ArrayList<MimeType>(targetMimeTypes);
	}

	/**
	 * Creates a converter that requires a specific content-type message header
	 *
	 * @param supportedSourceMimeType {@link MimeType} that must be present in content-type header
	 * @param targetMimeType the required target type
	 */
	protected AbstractFromMessageConverter(MimeType supportedSourceMimeType, MimeType targetMimeType) {
		this(Collections.singletonList(supportedSourceMimeType), targetMimeType, new StrictContentTypeResolver(
				supportedSourceMimeType));
	}

	/**
	 * Creates a converter that requires a specific content-type message header and supports multiple target MIME types.
	 *
	 * @param supportedSourceMimeType {@link MimeType} that must be present in content-type header
	 * @param targetMimeTypes a list of supported target types
	 */
	protected AbstractFromMessageConverter(MimeType supportedSourceMimeType, Collection<MimeType> targetMimeTypes) {
		this(Collections.singletonList(supportedSourceMimeType), targetMimeTypes, new StrictContentTypeResolver(
				supportedSourceMimeType));
	}

	/**
	 * Subclasses implement this to specify supported target types
	 *
	 * @return an array of supported classes or null if any target type is supported
	 */
	protected abstract Class<?>[] supportedTargetTypes();

	/**
	 * Subclasses implement this to specify supported payload types
	 *
	 * @return an array of supported classes or null if any target type is supported
	 */
	protected abstract Class<?>[] supportedPayloadTypes();

	protected boolean supportsPayloadType(Class<?> clazz) {
		return supportsType(clazz, supportedPayloadTypes());
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return supportsType(clazz, supportedTargetTypes());
	}

	private boolean supportsType(Class<?> clazz, Class<?>[] supportedTypes) {
		if (supportedTypes != null) {
			for (Class<?> targetType : supportedTypes) {
				if (ClassUtils.isAssignable(clazz, targetType)) {
					return true;
				}
			}
			return false;
		}
		return true;
	}

	@Override
	protected boolean canConvertFrom(Message<?> message, Class<?> targetClass) {
		return super.canConvertFrom(message, targetClass) && supportsPayloadType(message.getPayload().getClass());
	}

	public boolean supportsTargetMimeType(MimeType mimeType) {
		for (MimeType targetMimeType : targetMimeTypes) {
			if (mimeType.getType().equals(targetMimeType.getType()) && mimeType.getSubtype().equals(
					targetMimeType.getSubtype())) {
				return true;
			}
		}
		return false;
	}

	@Override
	// TODO: This will likely be fixed in core Spring
	public void setContentTypeResolver(ContentTypeResolver resolver) {
		if (getContentTypeResolver() == null) {
			super.setContentTypeResolver(resolver);
		}
	}

	/**
	 * Not supported by default
	 */
	@Override
	protected boolean canConvertTo(Object payload, MessageHeaders headers) {
		return false;
	}

	/**
	 * Not supported by default
	 */
	@Override
	public Object convertToInternal(Object payload, MessageHeaders headers) {
		throw new UnsupportedOperationException("'convertTo' not supported");
	}

	/**
	 * Convenience method to construct a converted message
	 *
	 * @param payload the converted payload
	 * @param headers the existing message headers
	 * @param contentType the value of the content-type header
	 * @return the converted message
	 */
	protected final Message<?> buildConvertedMessage(Object payload, MessageHeaders headers, MimeType contentType) {
		return MessageBuilder.withPayload(payload).copyHeaders(headers)
				.copyHeaders(
						Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
								contentType)).build();
	}
}
