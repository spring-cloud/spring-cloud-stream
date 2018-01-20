/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kstream.annotations.KStreamProcessor;
import org.springframework.cloud.stream.binder.kstream.config.KStreamApplicationSupportProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Soby Chacko
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
@DirtiesContext
public abstract class DeserializationErrorHandlerByKafkaTests {

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "counts", "error.words.group");

	@SpyBean
	KStreamBoundMessageConversionDelegate KStreamBoundMessageConversionDelegate;

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		System.setProperty("spring.cloud.stream.kstream.binder.brokers", embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kstream.binder.zkNodes", embeddedKafka.getZookeeperConnectionString());

		System.setProperty("server.port","0");
		System.setProperty("spring.jmx.enabled","false");

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("fooc", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "counts");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@SpringBootTest(properties = {
			"spring.cloud.stream.bindings.input.consumer.useNativeDecoding=true",
			"spring.cloud.stream.bindings.output.producer.useNativeEncoding=true",
			"spring.cloud.stream.bindings.input.group=group",
			"spring.cloud.stream.kstream.bindings.input.consumer.serdeError=sendToDlq",
			"spring.cloud.stream.kstream.binder.configuration.value.serde=" +
					"org.apache.kafka.common.serialization.Serdes$IntegerSerde"},
			webEnvironment= SpringBootTest.WebEnvironment.NONE
	)
	public static class DeserializationByKafkaAndDlqTests extends DeserializationErrorHandlerByKafkaTests {

		@Test
		@SuppressWarnings("unchecked")
		public void test() throws Exception {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("words");
			template.sendDefault("foobar");

			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("foobar", "false", embeddedKafka);
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
			Consumer<String, String> consumer1 = cf.createConsumer();
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer1, "error.words.group");

			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer1, "error.words.group");
			assertThat(cr.value().equals("foobar")).isTrue();

			//Ensuring that the deserialization was indeed done by Kafka natively
			verify(KStreamBoundMessageConversionDelegate, never()).deserializeOnInbound(any(Class.class), any(KStream.class));
			verify(KStreamBoundMessageConversionDelegate, never()).serializeOnOutbound(any(KStream.class));
		}
	}


	@EnableBinding(KStreamProcessor.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/kstream/integTest-1.properties")
	@EnableConfigurationProperties(KStreamApplicationSupportProperties.class)
	static class WordCountProcessorApplication {

		@Autowired
		private TimeWindows timeWindows;

		@StreamListener("input")
		@SendTo("output")
		public KStream<?, String> process(KStream<Object, String> input) {

			return input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Serdes.String(), Serdes.String())
					.count(timeWindows, "foo-WordCounts-x")
					.toStream()
					.map((key, value) -> new KeyValue<>(null, "Count for " + key.key() + " : " + value));
		}

	}
}
