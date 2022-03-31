/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.autoconfigure.context.ContextResourceLoaderAutoConfiguration;
import io.awspring.cloud.autoconfigure.context.ContextStackAutoConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.json.JacksonJsonUtils;
import org.springframework.integration.support.locks.DefaultLockRegistry;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = {
		"spring.cloud.stream.bindings.eventConsumerBatchProcessingWithHeaders-in-0.destination=" + KinesisBinderFunctionalTests.KINESIS_STREAM,
		"spring.cloud.stream.kinesis.bindings.eventConsumerBatchProcessingWithHeaders-in-0.consumer.idleBetweenPolls = 1",
		"spring.cloud.stream.kinesis.bindings.eventConsumerBatchProcessingWithHeaders-in-0.consumer.listenerMode = batch",
		"spring.cloud.stream.kinesis.bindings.eventConsumerBatchProcessingWithHeaders-in-0.consumer.checkpointMode = manual",
		"spring.cloud.stream.kinesis.binder.headers = event.eventType",
		"spring.cloud.stream.kinesis.binder.autoAddShards = true",
		"cloud.aws.region.static=eu-west-2" })
@DirtiesContext
public class KinesisBinderFunctionalTests implements LocalstackContainerTest {

	static final String KINESIS_STREAM = "test_stream";

	private static AmazonKinesisAsync AMAZON_KINESIS;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private CountDownLatch messageBarrier;

	@Autowired
	private AtomicReference<Message<List<?>>> messageHolder;

	@BeforeAll
	static void setup() {
		AMAZON_KINESIS = LocalstackContainerTest.kinesisClient();
		System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
	}

	@Test
	void testKinesisFunction() throws JsonProcessingException, InterruptedException {
		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(KINESIS_STREAM);
		List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			Message<String> eventMessages =
				MessageBuilder.withPayload("Message" + i)
					.setHeader("event.eventType", "createEvent")
					.build();
			PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
			byte[] jsonInput = objectMapper.writeValueAsBytes(eventMessages);
			putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonInput));
			putRecordsRequestEntry.setPartitionKey("1");
			putRecordsRequestEntryList.add(putRecordsRequestEntry);
		}
		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		AMAZON_KINESIS.putRecords(putRecordsRequest);

		assertThat(this.messageBarrier.await(10, TimeUnit.SECONDS)).isTrue();

		Message<List<?>> message = this.messageHolder.get();
		assertThat(message.getHeaders())
			.containsKeys(AwsHeaders.CHECKPOINTER,
				AwsHeaders.SHARD,
				AwsHeaders.RECEIVED_PARTITION_KEY,
				AwsHeaders.RECEIVED_STREAM,
				AwsHeaders.RECEIVED_SEQUENCE_NUMBER)
			.doesNotContainKeys(AwsHeaders.STREAM, AwsHeaders.PARTITION_KEY);

		List<?> payload = message.getPayload();
		assertThat(payload).hasSize(10);

		Object item = payload.get(0);

		assertThat(item).isInstanceOf(GenericMessage.class);

		Message<?> messageFromBatch = (Message<?>) item;

		assertThat(messageFromBatch.getPayload()).isEqualTo("Message0");
		assertThat(messageFromBatch.getHeaders())
			.containsEntry("event.eventType", "createEvent");
	}

	@Configuration
	@EnableAutoConfiguration(exclude = {
		ContextResourceLoaderAutoConfiguration.class,
		ContextStackAutoConfiguration.class })
	static class TestConfiguration {


		@Bean(destroyMethod = "")
		public AmazonKinesisAsync amazonKinesis() {
			return AMAZON_KINESIS;
		}

		@Bean
		public LockRegistry lockRegistry() {
			return new DefaultLockRegistry();
		}

		@Bean
		public ConcurrentMetadataStore checkpointStore() {
			return new SimpleMetadataStore();
		}

		@Bean
		public ObjectMapper objectMapper() {
			return JacksonJsonUtils.messagingAwareMapper();
		}

		@Bean
		public AtomicReference<Message<List<?>>> messageHolder() {
			return new AtomicReference<>();
		}

		@Bean
		public CountDownLatch messageBarrier() {
			return new CountDownLatch(1);
		}

		@Bean
		public Consumer<Message<List<?>>> eventConsumerBatchProcessingWithHeaders() {
			return eventMessages -> {
				messageHolder().set(eventMessages);
				messageBarrier().countDown();
			};
		}

	}

}
