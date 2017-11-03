/*
 * Copyright 2015-2017 the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;

/**
 * A factory for creating an instance of {@link CompositeMessageConverter} for a given
 * target MIME type
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
public class CompositeMessageConverterFactory {

	private final Log log = LogFactory.getLog(CompositeMessageConverterFactory.class);

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

		MappingJackson2MessageConverter jsonMessageConverter = new MappingJackson2MessageConverter() {
			@Override
			protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers, @Nullable Object conversionHint) {
				if (payload instanceof byte[]){
					return payload;
				}
				else if (payload instanceof String) {
					return ((String)payload).getBytes(StandardCharsets.UTF_8);
				}
				else {
					return super.convertToInternal(payload, headers, conversionHint);
				}
			}

			@Override
			protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
				try{
					return super.convertFromInternal(message, targetClass, conversionHint);
				} catch (MessageConversionException me){
					//Strings need special treatment
					if(targetClass.isAssignableFrom(String.class)){
						return message.getPayload();
					}
					throw me;
				}
			}
		};
		jsonMessageConverter.setStrictContentTypeMatch(true);
		if (this.objectMapper != null) {
			jsonMessageConverter.setObjectMapper(this.objectMapper);
		}

		this.converters.add(jsonMessageConverter);
		this.converters.add(new ByteArrayMessageConverter());
		this.converters.add(new ObjectStringMessageConverter());
		this.converters.add(new JavaSerializationMessageConverter());
		this.converters.add(new KryoMessageConverter(null,true));
		this.converters.add(new JsonUnmarshallingConverter(this.objectMapper));
	}

	/**
	 * Creation method.
	 * @param mimeType the target MIME type
	 * @return a converter for the target MIME type
	 */
	public MessageConverter getMessageConverterForType(MimeType mimeType) {
		List<MessageConverter> converters = new ArrayList<>();
		for (MessageConverter converter : this.converters) {
			if (converter instanceof AbstractMessageConverter) {
				for (MimeType type : ((AbstractMessageConverter) converter).getSupportedMimeTypes()) {
					if (type.includes(mimeType)) {
						converters.add(converter);
					}
				}
			}
			else {
				if (this.log.isDebugEnabled()) {
					this.log.debug("Ommitted " + converter + " of type " + converter.getClass().toString() +
							" for '" + mimeType.toString() + "' as it is not an AbstractMessageConverter");
				}
			}
		}
		if (CollectionUtils.isEmpty(converters)) {
			throw new ConversionException("No message converter is registered for "
					+ mimeType.toString());
		}
		if (converters.size() > 1) {
			return new CompositeMessageConverter(converters);
		}
		else {
			return converters.get(0);
		}
	}

	public CompositeMessageConverter getMessageConverterForAllRegistered() {
		return new CompositeMessageConverter(new ArrayList<>(this.converters));
	}

}
