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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 * @since 4.1.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
	"spring.cloud.stream.bindings.kafka-binding-service-test.producer.partition-count=2",
	"spring.cloud.stream.bindings.kafka-binding-service-test.producer.partition-key-expression=headers['partitionKey']"})
@DirtiesContext
@EmbeddedKafka(topics = "kafka-binding-service-test", controlledShutdown = true, partitions = 4,
	bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class KafkaBindingServiceTests {

	@Autowired
	private ApplicationContext context;

	@Test
	void testKafkaBinderOverridesBindingPartitionCountToTopicPartitionsIfHigher() {
		final StreamBridge streamBridge = this.context.getBean(StreamBridge.class);
		GenericMessage<String> message = new GenericMessage<>("foo", Map.of("partitionKey", "key1"));
		streamBridge.send("kafka-binding-service-test", message);
		BindingServiceProperties bindingServiceProperties = context.getBean(BindingServiceProperties.class);
		ProducerProperties producerProperties = bindingServiceProperties.getProducerProperties("kafka-binding-service-test");
		// Verify that the partition count on the binding is updated to the topic partition count
		// since the topic was created with more partitions than partition-count on the producer binding.
		assertThat(producerProperties.getPartitionCount()).isEqualTo(4);
	}

	@EnableAutoConfiguration
	@Configuration
	public static class Config {

	}
}
