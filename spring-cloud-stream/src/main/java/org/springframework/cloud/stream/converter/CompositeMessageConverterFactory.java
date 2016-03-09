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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
import org.springframework.util.ObjectUtils;


/**
 * A factory for creating an instance of {@link CompositeMessageConverter} for a given target MIME type
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
public class CompositeMessageConverterFactory {

	private final List<AbstractFromMessageConverter> converters;

	public CompositeMessageConverterFactory() {
		this(Collections.<AbstractFromMessageConverter>emptyList());
	}

	/**
	 * @param customConverters a list of {@link AbstractFromMessageConverter}
	 */
	public CompositeMessageConverterFactory(List<? extends AbstractFromMessageConverter> customConverters) {
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
		this.converters.add(new TupleToJsonMessageConverter());
		this.converters.add(new JsonToPojoMessageConverter());
		this.converters.add(new PojoToJsonMessageConverter());
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
	public CompositeMessageConverter getMessageConverterForType(MimeType targetMimeType) {
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

	public CompositeMessageConverter getMessageConverterForAllRegistered() {
		return new CompositeMessageConverter(new ArrayList<MessageConverter>(converters));
	}

	public Class<?>[] supportedDataTypes(MimeType targetMimeType) {
		Set<Class<?>> supportedDataTypes = new HashSet<>();
		// Make sure to check if the target type is of explicit java object type.
		if (MessageConverterUtils.X_JAVA_OBJECT.includes(targetMimeType)) {
			supportedDataTypes.add(MessageConverterUtils.getJavaTypeForJavaObjectContentType(targetMimeType));
		}
		else {
			for (AbstractFromMessageConverter converter : converters) {
				if (converter.supportsTargetMimeType(targetMimeType)) {
					Class<?>[] targetTypes = converter.supportedTargetTypes();
					if (!ObjectUtils.isEmpty(targetTypes)) {
						supportedDataTypes.addAll(Arrays.asList(targetTypes));
					}
				}
			}
		}
		return supportedDataTypes.toArray(new Class<?>[supportedDataTypes.size()]);
	}
}
