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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
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
		this(Collections.<AbstractFromMessageConverter>emptyList(), new ObjectMapper());
	}

	/**
	 * @param customConverters a list of {@link AbstractFromMessageConverter}
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
		this.converters.add(new JsonToTupleMessageConverter());
		this.converters.add(new TupleToJsonMessageConverter(objectMapper));
		this.converters.add(new JsonToPojoMessageConverter(objectMapper));
		this.converters.add(new PojoToJsonMessageConverter(objectMapper));
		this.converters.add(new ByteArrayToStringMessageConverter());
		this.converters.add(new StringToByteArrayMessageConverter());
		this.converters.add(new PojoToStringMessageConverter());
		this.converters.add(new JavaToSerializedMessageConverter());
		this.converters.add(new SerializedToJavaMessageConverter());
	}

	/**
	 * Creation method.
	 * @param targetMimeType the target MIME type
	 * @return a converter for the target MIME type
	 */
	public CompositeMessageConverter getMessageConverterForTargetType(MimeType targetMimeType) {
		List<MessageConverter> targetMimeTypeConverters = new ArrayList<MessageConverter>();
		for (MessageConverter converter : converters) {
			if (converter instanceof TargetTypeMessageConverter) {
				TargetTypeMessageConverter targetTypeMessageConverter = (TargetTypeMessageConverter) converter;
				if (targetTypeMessageConverter.supportsTargetMimeType(targetMimeType)) {
					targetMimeTypeConverters.add(converter);
				}
			}
		}
		if (CollectionUtils.isEmpty(targetMimeTypeConverters)) {
			throw new ConversionException("No message converter is registered for "
					+ targetMimeType.toString());
		}
		return new CompositeMessageConverter(targetMimeTypeConverters);
	}

	public CompositeMessageConverter getMessageConverterForAllRegistered() {
		return new CompositeMessageConverter(new ArrayList<>(converters));
	}
}
