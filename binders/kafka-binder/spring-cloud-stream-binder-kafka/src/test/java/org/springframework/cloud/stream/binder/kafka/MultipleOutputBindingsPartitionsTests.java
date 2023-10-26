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

package org.springframework.cloud.stream.binder.kafka;

import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/***
 * @author Soby Chacko
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka
class MultipleOutputBindingsPartitionsTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@BeforeEach
	public void before() {
		NewTopic newTopic1 = new NewTopic("odd-topic", 10, (short) 1);
		NewTopic newTopic2 = new NewTopic("even-topic", 5, (short) 1);
		embeddedKafka.addTopics(newTopic1, newTopic2);
	}

	@Test
	void singleInputTupleOutputWithDifferentPartitions() {
		try (ConfigurableApplicationContext context = new SpringApplicationBuilder(MultiOutputApplication.class)
			.web(WebApplicationType.NONE).run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.kafka.consumer.metadata.max.age.ms=1000",
				"--spring.cloud.function.definition=singleInputMultipleOutputs",
				"--spring.cloud.stream.function.reactive.singleInputMultipleOutputs=true",
				"--spring.cloud.stream.kafka.binder.autoAddPartitions=true",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-in-0.group=grp5",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-in-0.destination=multi-input",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-0.destination=odd-topic",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-1.destination=even-topic",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-0.producer.partitionKeyExpression=payload",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-0.producer.partitionCount=10",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-0.producer.partitionSelectorName=mySelectorStrategy",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-1.producer.partitionKeyExpression=payload",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-1.producer.partitionCount=5",
				"--spring.cloud.stream.bindings.singleInputMultipleOutputs-out-1.producer.partitionSelectorName=mySelectorStrategy",
				"--spring.cloud.stream.kafka.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			streamBridge.send("multi-input", MessageBuilder.withPayload(101)
				.build());
			streamBridge.send("multi-input", MessageBuilder.withPayload(102)
				.build());

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group3", "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromEmbeddedTopics(consumer1, "odd-topic");
			Consumer<String, String> consumer2 = cf.createConsumer("group4", null);
			embeddedKafka.consumeFromEmbeddedTopics(consumer2, "even-topic");

			ConsumerRecord<String, String> record1 = KafkaTestUtils.getSingleRecord(consumer1, "odd-topic");
			assertThat(record1)
				.isNotNull()
				.extracting(ConsumerRecord::value)
				.isEqualTo("ODD: 101");
			assertThat(record1)
				.extracting(ConsumerRecord::partition)
				.isEqualTo(9);

			ConsumerRecord<String, String> record2 = KafkaTestUtils.getSingleRecord(consumer2, "even-topic");
			assertThat(record2)
				.isNotNull()
				.extracting(ConsumerRecord::value)
				.isEqualTo("EVEN: 102");
			assertThat(record2)
				.extracting(ConsumerRecord::partition)
				.isEqualTo(4);
		}
	}


	@EnableAutoConfiguration
	@Configuration
	public static class MultiOutputApplication {

		@Bean
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public static Function<Flux<Integer>, Tuple2<Flux<String>, Flux<String>>> singleInputMultipleOutputs() {
			return flux -> {
				Flux<Integer> connectedFlux = flux.publish().autoConnect(2);
				UnicastProcessor odd = UnicastProcessor.create();
				UnicastProcessor even = UnicastProcessor.create();
				Flux<Integer> oddFlux = connectedFlux.filter(number -> number % 2 != 0).doOnNext(number -> odd.onNext("ODD: " + number));
				Flux<Integer> evenFlux = connectedFlux.filter(number -> number % 2 == 0).doOnNext(number -> even.onNext("EVEN: " + number));
				return Tuples.of(Flux.from(odd).doOnSubscribe(x -> oddFlux.subscribe()), Flux.from(even).doOnSubscribe(x -> evenFlux.subscribe()));
			};
		}

		@Bean
		public PartitionSelectorStrategy mySelectorStrategy() {
			return new MyPartitionSelector();
		}
	}

	static class MyPartitionSelector implements PartitionSelectorStrategy {

		@Override
		public int selectPartition(Object key, int partitionCount) {
			// selecting the last partition for easy test verification.
			return partitionCount - 1;
		}
	}
}
