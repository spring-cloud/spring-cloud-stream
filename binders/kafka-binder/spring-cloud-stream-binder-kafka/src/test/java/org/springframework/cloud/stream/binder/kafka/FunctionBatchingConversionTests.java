/*
 * Copyright 2024-2024 the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
	"spring.cloud.function.definition=batchConsumer",
	"spring.cloud.stream.bindings.batchConsumer-in-0.consumer.batch-mode=true",
	"spring.cloud.stream.bindings.batchConsumer-in-0.destination=cfrthp-topic",
	"spring.cloud.stream.bindings.batchConsumer-in-0.group=cfrthp-group"
})
@EmbeddedKafka
@DirtiesContext
public class FunctionBatchingConversionTests {

	@Autowired
	private StreamBridge streamBridge;

	static CountDownLatch latch = new CountDownLatch(3);

	static List<Map<String, Object>> batchConvertedHeaders = new ArrayList<>();

	@Test
	void conversionFailuresRemoveTheHeadersProperly() throws Exception {
		streamBridge.send("cfrthp-topic", MessageBuilder.withPayload("hello".getBytes(StandardCharsets.UTF_8))
			.setHeader("index", 0).build());
		streamBridge.send("cfrthp-topic", MessageBuilder.withPayload("{\"name\":\"Ricky\"}".getBytes(StandardCharsets.UTF_8))
			.setHeader("index", 1).build());
		streamBridge.send("cfrthp-topic", MessageBuilder.withPayload("{\"name\":\"Julian\"}".getBytes(StandardCharsets.UTF_8))
			.setHeader("index", 2).build());
		streamBridge.send("cfrthp-topic", MessageBuilder.withPayload("hello".getBytes(StandardCharsets.UTF_8))
			.setHeader("index", 3).build());
		streamBridge.send("cfrthp-topic", MessageBuilder.withPayload("{\"name\":\"Bubbles\"}".getBytes(StandardCharsets.UTF_8))
			.setHeader("index", 4).build());

		Assert.isTrue(latch.await(10, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(batchConvertedHeaders.size()).isEqualTo(3);

		assertThat(batchConvertedHeaders.get(0).get("index")).isEqualTo(1);
		assertThat(batchConvertedHeaders.get(1).get("index")).isEqualTo(2);
		assertThat(batchConvertedHeaders.get(2).get("index")).isEqualTo(4);
	}

	@EnableAutoConfiguration
	@Configuration
	public static class Config {

		@Bean
		@SuppressWarnings("unchecked")
		Consumer<Message<List<Person>>> batchConsumer() {
			return message -> {
				if (!message.getPayload().isEmpty()) {
					List<Map<String, Object>> o = (List<Map<String, Object>>) message.getHeaders().get("kafka_batchConvertedHeaders");
					batchConvertedHeaders.addAll(o);
					message.getPayload().forEach(c -> {
						latch.countDown();
					});
				}
			};
		}
	}

	record Person(String name) {
	}

}
