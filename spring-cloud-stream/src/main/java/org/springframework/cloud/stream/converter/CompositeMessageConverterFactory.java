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
import java.util.List;

import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;


/**
 * A factory for creating an instance of {@link CompositeMessageConverter} for a given target MIME type
 *
 * @author David Turanski
 */
public class CompositeMessageConverterFactory {

	private final List<AbstractFromMessageConverter> converters;

	/**
	 * @param converters a list of {@link AbstractFromMessageConverter}
	 */
	public CompositeMessageConverterFactory(Collection<AbstractFromMessageConverter> converters) {
		Assert.notNull(converters, "'converters' cannot be null");
		this.converters = new ArrayList<AbstractFromMessageConverter>(converters);
	}

	/**
	 * Creation method.
	 *
	 * @param targetMimeType the target MIME type
	 * @return a converter for the target MIME type
	 */
	public CompositeMessageConverter newInstance(MimeType targetMimeType) {
		List<MessageConverter> targetMimeTypeConverters = new ArrayList<MessageConverter>();
		for (AbstractFromMessageConverter converter : converters) {
			if (converter.supportsTargetMimeType(targetMimeType)) {
				targetMimeTypeConverters.add(converter);
			}
		}
		if (CollectionUtils.isEmpty(targetMimeTypeConverters)) {
			throw new ConversionException("No message converter is registered for "
					+ targetMimeType.toString());
		}
		return new CompositeMessageConverter(targetMimeTypeConverters);
	}
}
