/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration.topic.configs;

import java.util.function.Function;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Heiko Does
 * @author Soby Chacko
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
		classes = BaseKafkaBinderTopicPropertiesUpdateTest.TopicAutoConfigsTestConfig.class,
		webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.cloud.stream.function.bindings.process-in-0=standard-in",
		"spring.cloud.stream.function.bindings.process-out-0=standard-out",
		"spring.cloud.stream.kafka.bindings.standard-out.producer.topic.properties.retention.ms=9001",
		"spring.cloud.stream.kafka.default.producer.topic.properties.retention.ms=-1",
		"spring.cloud.stream.kafka.bindings.standard-in.consumer.topic.properties.retention.ms=9001",
		"spring.cloud.stream.kafka.default.consumer.topic.properties.retention.ms=-1"
})
@DirtiesContext
public abstract class BaseKafkaBinderTopicPropertiesUpdateTest {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.cloud.stream.kafka.binder.brokers";

	@ClassRule
	public static EmbeddedKafkaRule kafkaEmbedded = new EmbeddedKafkaRule(1, true, "standard-in", "standard-out");

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				kafkaEmbedded.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@EnableAutoConfiguration
	public static class TopicAutoConfigsTestConfig {

		@Bean
		public Function<String, String> process() {
			return payload -> payload;
		}
	}
}
