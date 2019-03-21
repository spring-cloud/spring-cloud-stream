/*
 * Copyright 2015-2017 the original author or authors.
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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.tuple.Tuple;
import org.springframework.tuple.TupleBuilder;
import org.springframework.util.MimeTypeUtils;

/**
 * A {@link org.springframework.messaging.converter.MessageConverter} to convert a
 * {@link Tuple} to JSON bytes.
 *
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @deprecated as of 2.0. please use 'application/json' content type
 */
@Deprecated
public class TupleJsonMessageConverter extends AbstractMessageConverter {

	private final ObjectMapper objectMapper;

	@Value("${typeconversion.json.prettyPrint:false}")
	private volatile boolean prettyPrint;

	public TupleJsonMessageConverter(ObjectMapper objectMapper) {
		super(Arrays.asList(MessageConverterUtils.X_SPRING_TUPLE,
				MimeTypeUtils.APPLICATION_JSON));
		this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
	}

	public void setPrettyPrint(boolean prettyPrint) {
		this.prettyPrint = prettyPrint;
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return Tuple.class.isAssignableFrom(clazz);
	}

	@Override
	protected Object convertToInternal(Object payload, MessageHeaders headers,
			Object conversionHint) {
		Tuple t = (Tuple) payload;
		String json;
		if (this.prettyPrint) {
			try {
				Object tmp = this.objectMapper.readValue(t.toString(), Object.class);
				json = this.objectMapper.writerWithDefaultPrettyPrinter()
						.writeValueAsString(tmp);
			}
			catch (IOException e) {
				this.logger.error(e.getMessage(), e);
				return null;
			}
		}
		else {
			json = t.toString();
		}
		return json.getBytes();
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass,
			Object conversionHint) {
		String source;
		if (message.getPayload() instanceof byte[]) {
			source = new String((byte[]) message.getPayload(), Charset.forName("UTF-8"));
		}
		else {
			source = message.getPayload().toString();
		}
		return TupleBuilder.fromString(source);
	}

}
