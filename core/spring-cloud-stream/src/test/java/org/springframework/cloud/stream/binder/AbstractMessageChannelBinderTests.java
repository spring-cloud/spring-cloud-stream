/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic unit tests for {@link AbstractMessageChannelBinder}.
 *
 * @author Soby Chacko
 */
public class AbstractMessageChannelBinderTests {

	@Test
	@SuppressWarnings("unchecked")
	void serializeDurationOnObjectMapperInAMCB() throws Exception {

		AbstractMessageChannelBinder<?, ?, ?> binder = new AbstractMessageChannelBinder<>(null, null) {
			@Override
			protected MessageHandler createProducerMessageHandler(ProducerDestination destination, ProducerProperties producerProperties, MessageChannel errorChannel) {
				return null;
			}

			@Override
			protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group, ConsumerProperties properties) {
				return null;
			}
		};
		final GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.refresh();
		binder.setApplicationContext(applicationContext);
		binder.onInit();

		Field objectMapperField = ReflectionUtils.findField(AbstractMessageChannelBinder.class, "objectMapper");
		assertThat(objectMapperField).isNotNull();
		ReflectionUtils.makeAccessible(objectMapperField);
		final ObjectMapper objectMapper = (ObjectMapper) ReflectionUtils.getField(objectMapperField, binder);
		assertThat(objectMapper).isNotNull();

		Duration duration = Duration.ofHours(1);
		Map<String, Object> properties = Map.of("foo", duration);
		final Map<String, Object> convertedMap = objectMapper.convertValue(properties, Map.class);

		assertThat(convertedMap).isNotEmpty();
	}
}
