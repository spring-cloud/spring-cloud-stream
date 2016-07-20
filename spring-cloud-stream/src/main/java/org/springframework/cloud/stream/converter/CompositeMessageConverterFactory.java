/*
 * Copyright 2015-2016 the original author or authors.
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
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;


/**
 * A factory for creating an instance of {@link CompositeMessageConverter} for a given target MIME type
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
public class CompositeMessageConverterFactory {

	private final ObjectMapper objectMapper;

	private final List<MessageConverter> converters;

	public CompositeMessageConverterFactory() {
		this(Collections.<MessageConverter>emptyList(), new ObjectMapper());
	}

	/**
	 * @param customConverters a list of {@link AbstractMessageConverter}
	 */
	public CompositeMessageConverterFactory(List<? extends MessageConverter> customConverters,
			ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
		if (!CollectionUtils.isEmpty(customConverters)) {
			this.converters = new ArrayList<>(customConverters);
		}
		else {
			this.converters = new ArrayList<>();
		}
		initDefaultConverters();
	}


	private void initDefaultConverters() {
		this.converters.add(new TupleJsonMessageConverter(this.objectMapper));

		MappingJackson2MessageConverter jsonMessageConverter = new MappingJackson2MessageConverter();
		jsonMessageConverter.setSerializedPayloadClass(String.class);
		if (this.objectMapper != null) {
			jsonMessageConverter.setObjectMapper(this.objectMapper);
		}

		this.converters.add(jsonMessageConverter);
		this.converters.add(new ByteArrayMessageConverter());

		this.converters.add(new StringMessageConverter());
		this.converters.add(new JavaSerializationMessageConverter());
	}

	/**
	 * Creation method.
	 * @param mimeType the target MIME type
	 * @return a converter for the target MIME type
	 */
	public CompositeMessageConverter getMessageConverterForType(MimeType mimeType) {
		List<MessageConverter> converters = new ArrayList<>();
		for (MessageConverter converter : this.converters) {
			if (converter instanceof AbstractMessageConverter) {
				for (MimeType type : ((AbstractMessageConverter) converter).getSupportedMimeTypes()) {
					if (type.includes(mimeType)) {
						converters.add(converter);
					}
				}
			}
		}
		if (CollectionUtils.isEmpty(converters)) {
			throw new ConversionException("No message converter is registered for "
					+ mimeType.toString());
		}
		return new CompositeMessageConverter(converters);
	}

	public CompositeMessageConverter getMessageConverterForAllRegistered() {
		return new CompositeMessageConverter(new ArrayList<>(this.converters));
	}

}
