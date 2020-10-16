/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.serde;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.example.Sensor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.springframework.cloud.schema.registry.avro.AvroSchemaMessageConverter;
import org.springframework.cloud.schema.registry.avro.AvroSchemaServiceManagerImpl;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.messaging.converter.MessageConverter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Refer {@link MessageConverterDelegateSerde} for motivations.
 *
 * @author Soby Chacko
 */
public class MessageConverterDelegateSerdeTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testCompositeNonNativeSerdeUsingAvroContentType() {
		Random random = new Random();
		Sensor sensor = new Sensor();
		sensor.setId(UUID.randomUUID().toString() + "-v1");
		sensor.setAcceleration(random.nextFloat() * 10);
		sensor.setVelocity(random.nextFloat() * 100);
		sensor.setTemperature(random.nextFloat() * 50);

		List<MessageConverter> messageConverters = new ArrayList<>();
		messageConverters.add(new AvroSchemaMessageConverter(new AvroSchemaServiceManagerImpl()));
		CompositeMessageConverterFactory compositeMessageConverterFactory = new CompositeMessageConverterFactory(
				messageConverters, new ObjectMapper());
		MessageConverterDelegateSerde messageConverterDelegateSerde = new MessageConverterDelegateSerde(
				compositeMessageConverterFactory.getMessageConverterForAllRegistered());

		Map<String, Object> configs = new HashMap<>();
		configs.put("valueClass", Sensor.class);
		configs.put("contentType", "application/avro");
		messageConverterDelegateSerde.configure(configs, false);
		final byte[] serialized = messageConverterDelegateSerde.serializer().serialize(null,
				sensor);

		final Object deserialized = messageConverterDelegateSerde.deserializer()
				.deserialize(null, serialized);

		assertThat(deserialized).isEqualTo(sensor);
	}

}
