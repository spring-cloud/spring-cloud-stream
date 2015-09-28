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

import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.util.MimeTypeUtils;
import org.springframework.cloud.stream.tuple.Tuple;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * A {@link org.springframework.messaging.converter.MessageConverter}
 * to convert a {@link Tuple} to a JSON String
 *
 * @author David Turanski
 */
public class TupleToJsonMessageConverter extends AbstractFromMessageConverter {

	@Value("${typeconversion.json.prettyPrint:false}")
	private volatile boolean prettyPrint;

	public void setPrettyPrint(boolean prettyPrint) {
		this.prettyPrint = prettyPrint;
	}

	public TupleToJsonMessageConverter() {
		super(MimeTypeUtils.APPLICATION_JSON);
	}

	@Override
	protected Class<?>[] supportedTargetTypes() {
		return new Class<?>[] { String.class };
	}

	@Override
	protected Class<?>[] supportedPayloadTypes() {
		return new Class<?>[] { Tuple.class };
	}

	@Override
	public Object convertFromInternal(Message<?> message, Class<?> targetClass) {
		Tuple t = (Tuple) message.getPayload();
		String json;
		if (prettyPrint) {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
			try {
				Object tmp = mapper.readValue(t.toString(), Object.class);
				json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(tmp);
			}
			catch (IOException e) {
				logger.error(e.getMessage(), e);
				return null;
			}
		}
		else {
			json = t.toString();
		}
		return buildConvertedMessage(json, message.getHeaders(), MimeTypeUtils.APPLICATION_JSON);
	}

}
