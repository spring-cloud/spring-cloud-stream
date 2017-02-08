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

package org.springframework.cloud.stream.binder.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * An {@link EnvironmentPostProcessor} that sets some common configuration properties (log config etc.,) for Kafka
 * binder.
 *
 * @author Ilayaperumal Gopinathan
 */
public class KafkaBinderEnvironmentPostProcessor implements EnvironmentPostProcessor {

	public final static String SPRING_KAFKA = "spring.kafka";

	public final static String SPRING_KAFKA_PRODUCER = SPRING_KAFKA + ".producer";

	public final static String SPRING_KAFKA_CONSUMER = SPRING_KAFKA + ".consumer";

	public final static String SPRING_KAFKA_PRODUCER_KEY_SERIALIZER = SPRING_KAFKA_PRODUCER + "." + "keySerializer";

	public final static String SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER = SPRING_KAFKA_PRODUCER + "." + "valueSerializer";

	public final static String SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER = SPRING_KAFKA_CONSUMER + "." + "keyDeserializer";

	public final static String SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER = SPRING_KAFKA_CONSUMER + "." + "valueDeserializer";

	public final static String SPRING_KAFKA_BOOTSTRAP_SERVERS = SPRING_KAFKA + "." + "bootstrapServers";

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		Map<String, Object> logProperties = new HashMap<>();
		logProperties.put("logging.pattern.console", "%d{ISO8601} %5p %t %c{2}:%L - %m%n");
		logProperties.put("logging.level.org.I0Itec.zkclient", "ERROR");
		logProperties.put("logging.level.kafka.server.KafkaConfig", "ERROR");
		logProperties.put("logging.level.kafka.admin.AdminClient.AdminConfig", "ERROR");
		environment.getPropertySources().addLast(new MapPropertySource("kafkaBinderLogConfig", logProperties));
		Map<String, Object> binderConfig = new HashMap<>();
		if (environment.getProperty(SPRING_KAFKA_PRODUCER_KEY_SERIALIZER) != null) {
			binderConfig.put(SPRING_KAFKA_PRODUCER_KEY_SERIALIZER, environment.getProperty(SPRING_KAFKA_PRODUCER_KEY_SERIALIZER));
		}
		else {
			binderConfig.put(SPRING_KAFKA_PRODUCER_KEY_SERIALIZER, ByteArraySerializer.class);
		}
		if (environment.getProperty(SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER) != null) {
			binderConfig.put(SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER, environment.getProperty(SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER));
		}
		else {
			binderConfig.put(SPRING_KAFKA_PRODUCER_VALUE_SERIALIZER, ByteArraySerializer.class);
		}
		if (environment.getProperty(SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER) != null) {
			binderConfig.put(SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER, environment.getProperty(SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER));
		}
		else {
			binderConfig.put(SPRING_KAFKA_CONSUMER_KEY_DESERIALIZER, ByteArrayDeserializer.class);
		}
		if (environment.getProperty(SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER) != null) {
			binderConfig.put(SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER, environment.getProperty(SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER));
		}
		else {
			binderConfig.put(SPRING_KAFKA_CONSUMER_VALUE_DESERIALIZER, ByteArrayDeserializer.class);
		}
		if (environment.getProperty(SPRING_KAFKA_BOOTSTRAP_SERVERS) != null) {
			binderConfig.put(SPRING_KAFKA_BOOTSTRAP_SERVERS, environment.getProperty(SPRING_KAFKA_BOOTSTRAP_SERVERS));
		}
		else {
			binderConfig.put(SPRING_KAFKA_BOOTSTRAP_SERVERS, "");
		}
		environment.getPropertySources().addLast(new MapPropertySource("kafkaBinderConfig", binderConfig));
	}
}
