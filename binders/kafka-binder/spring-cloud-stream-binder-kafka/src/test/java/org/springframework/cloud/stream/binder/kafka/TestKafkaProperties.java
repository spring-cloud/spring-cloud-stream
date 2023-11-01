/*
 * Copyright 2018-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Test {@link KafkaProperties} initialized in the same way as the
 * {@code KafkaBinderEnvironmentPostProcessor} initializes the properties.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
class TestKafkaProperties extends KafkaProperties {

	TestKafkaProperties() {
		getConsumer().setKeyDeserializer(ByteArrayDeserializer.class);
		getConsumer().setValueDeserializer(ByteArrayDeserializer.class);
		getProducer().setKeySerializer(ByteArraySerializer.class);
		getProducer().setValueSerializer(ByteArraySerializer.class);
	}

}
