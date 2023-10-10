/*
 * Copyright 2015-2022 the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.function.context.config.JsonMessageConverter;
import org.springframework.cloud.function.json.JacksonMapper;
import org.springframework.cloud.function.json.JsonMapper;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeType;
/**
 * A factory for creating an instance of {@link CompositeMessageConverter} for a given
 * target MIME type.
 *
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

	private final JsonMapper jsonMapper;

	public CompositeMessageConverterFactory() {
		this(Collections.<MessageConverter>emptyList(), new ObjectMapper(), null);
	}

	/**
	 * @param customConverters a list of {@link AbstractMessageConverter}
	 * @param objectMapper object mapper for for serialization / deserialization
	 */
	public CompositeMessageConverterFactory(
			List<? extends MessageConverter> customConverters,
			ObjectMapper objectMapper, JsonMapper jsonMapper) {
		this.objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
		this.jsonMapper = jsonMapper == null ? new JacksonMapper(this.objectMapper) : jsonMapper;
		if (!CollectionUtils.isEmpty(customConverters)) {
			this.converters = new ArrayList<>(customConverters);
		}
		else {
			this.converters = new ArrayList<>();
		}
		initDefaultConverters();

		DefaultContentTypeResolver resolver = new DefaultContentTypeResolver() {
			@Override
			public MimeType resolve(@Nullable MessageHeaders headers) {
				Map<String, Object> messageHeaders = new HashMap<>(headers);
				Object contentType = headers.get(MessageHeaders.CONTENT_TYPE);
				if (contentType instanceof byte[]) {
					contentType = new String((byte[]) contentType, StandardCharsets.UTF_8);
					contentType = ((String) contentType).replace("\"", "");
					messageHeaders.put(MessageHeaders.CONTENT_TYPE, contentType);
				}
				return super.resolve(new MessageHeaders(messageHeaders));
			}
		};
		resolver.setDefaultMimeType(BindingProperties.DEFAULT_CONTENT_TYPE);
		this.converters.stream().filter(mc -> mc instanceof AbstractMessageConverter)
				.forEach(mc -> ((AbstractMessageConverter) mc)
						.setContentTypeResolver(resolver));
	}

	private void initDefaultConverters() {
		this.converters.add(new JsonMessageConverter(this.jsonMapper) {
			@Override
			protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers,
					@Nullable Object conversionHint) {
				/*
				 * We must revisit this. This is a copy from ApplicationMarshallingMessageConverter which derived from an older class etc. . .
				 * This attempts to use JSON conversion to convert something that is not json in the first place.
				 * For example Integer payload with application/json CT should actually fail since Integer is not a JSON.
				 * This is !!!!wrong!!!!! and ONLY remains here for backward compatibility.
				 */
				if (payload instanceof String) {
					return ((String) payload).getBytes(StandardCharsets.UTF_8);
				}
				return super.convertToInternal(payload, headers, conversionHint);
			}
		});
		this.converters.add(new ByteArrayMessageConverter() {
			@Override
			protected boolean supports(Class<?> clazz) {
				if (!super.supports(clazz)) {
					return (Object.class == clazz);
				}
				return true;
			}
		});
		this.converters.add(new ObjectStringMessageConverter());
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
				for (MimeType type : ((AbstractMessageConverter) converter)
						.getSupportedMimeTypes()) {
					if (type.includes(mimeType)) {
						converters.add(converter);
					}
				}
			}
			else {
				if (this.log.isDebugEnabled()) {
					this.log.debug("Ommitted " + converter + " of type "
							+ converter.getClass().toString() + " for '"
							+ mimeType.toString()
							+ "' as it is not an AbstractMessageConverter");
				}
			}
		}
		if (CollectionUtils.isEmpty(converters)) {
			throw new ConversionException(
					"No message converter is registered for " + mimeType.toString());
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
