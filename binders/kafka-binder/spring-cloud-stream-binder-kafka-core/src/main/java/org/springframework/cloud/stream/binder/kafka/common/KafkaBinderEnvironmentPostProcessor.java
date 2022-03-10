/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * An {@link EnvironmentPostProcessor} that sets some common configuration properties (log
 * config etc.,) for Kafka binder.
 *
 * @author Ilayaperumal Gopinathan
 */
public class KafkaBinderEnvironmentPostProcessor implements EnvironmentPostProcessor {

	private static final String SPRING_KAFKA = "spring.kafka";

	private static final String SPRING_KAFKA_PRODUCER = SPRING_KAFKA + ".producer";

	private static final String SPRING_KAFKA_CONSUMER = SPRING_KAFKA + ".consumer";

	private static final String SPRING_KAFKA_PRODUCER_KEY_SERIALIZER = SPRING_KAFKA_PRODUCER
			+ "." + "keySerializer";

	private static final String SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER = SPRING_KAFKA_PRODUCER
			+ "." + "valueSerializer";

	private static final String SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER = SPRING_KAFKA_CONSUMER
			+ "." + "keyDeserializer";

	private static final String SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER = SPRING_KAFKA_CONSUMER
			+ "." + "valueDeserializer";

	private static final String KAFKA_BINDER_DEFAULT_PROPERTIES = "kafkaBinderDefaultProperties";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment,
			SpringApplication application) {
		if (!environment.getPropertySources().contains(KAFKA_BINDER_DEFAULT_PROPERTIES)) {
			Map<String, Object> kafkaBinderDefaultProperties = new HashMap<>();
			kafkaBinderDefaultProperties.put("logging.level.org.I0Itec.zkclient",
					"ERROR");
			kafkaBinderDefaultProperties.put("logging.level.kafka.server.KafkaConfig",
					"ERROR");
			kafkaBinderDefaultProperties
					.put("logging.level.kafka.admin.AdminClient.AdminConfig", "ERROR");
			kafkaBinderDefaultProperties.put(SPRING_KAFKA_PRODUCER_KEY_SERIALIZER,
					ByteArraySerializer.class.getName());
			kafkaBinderDefaultProperties.put(SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER,
					ByteArraySerializer.class.getName());
			kafkaBinderDefaultProperties.put(SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER,
					ByteArrayDeserializer.class.getName());
			kafkaBinderDefaultProperties.put(SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER,
					ByteArrayDeserializer.class.getName());
			environment.getPropertySources().addLast(new MapPropertySource(
					KAFKA_BINDER_DEFAULT_PROPERTIES, kafkaBinderDefaultProperties));
		}
	}

}
