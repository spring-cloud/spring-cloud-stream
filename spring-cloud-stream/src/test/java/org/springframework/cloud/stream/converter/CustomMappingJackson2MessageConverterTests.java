/*
 * Copyright 2017 the original author or authors.
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

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Vinicius Carvalho
 */
public class CustomMappingJackson2MessageConverterTests {

	@Test
	public void convertFromStructuredJsonIntoPojo() throws Exception {
		String payload = "{\"id\":1}";
		Message message = MessageBuilder.withPayload(payload.getBytes()).setHeader(MessageHeaders.CONTENT_TYPE,"application/json").build();
		CustomJackson2MappingMessageConverter converter = new CustomJackson2MappingMessageConverter();
		Object converted = converter.convertFromInternal(message, Map.class,null);
		assertThat(((Map)converted).get("id")).isNotNull();
	}

	@Test
	public void convertFromStructuredJsonIntoString() throws Exception {
		String payload = "{\"id\":1}";
		Message message = MessageBuilder.withPayload(payload.getBytes()).setHeader(MessageHeaders.CONTENT_TYPE,"application/json").build();
		CustomJackson2MappingMessageConverter converter = new CustomJackson2MappingMessageConverter();
		Object converted = converter.convertFromInternal(message, String.class,null);
		assertThat(converted).isNotNull();
		assertThat(payload).isEqualTo(new String((byte[])converted));
	}

	@Test
	public void convertFromJsonString() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		String payload = mapper.writeValueAsString("foo");
		Message message = MessageBuilder.withPayload(payload.getBytes()).setHeader(MessageHeaders.CONTENT_TYPE,"application/json").build();
		CustomJackson2MappingMessageConverter converter = new CustomJackson2MappingMessageConverter();
		Object converted = converter.convertFromInternal(message, String.class,null);
		assertThat(converted).isNotNull();
		assertThat("foo").isEqualTo((String)converted);
	}
}
