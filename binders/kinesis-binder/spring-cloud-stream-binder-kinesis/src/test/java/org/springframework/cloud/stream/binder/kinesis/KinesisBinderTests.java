/*
 * Copyright 2017-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.BDDMockito;
import org.mockito.stubbing.Answer;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisConsumerProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.inbound.kinesis.KinesisShardOffset;
import org.springframework.integration.aws.inbound.kinesis.ListenerMode;
import org.springframework.integration.aws.support.AwsRequestFailureException;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

/**
 * The tests for Kinesis Binder.
 *
 * @author Artem Bilan
 * @author Jacob Severson
 * @author Arnaud Lecollaire
 */
public class KinesisBinderTests extends
	PartitionCapableBinderTests<KinesisTestBinder, ExtendedConsumerProperties<KinesisConsumerProperties>,
		ExtendedProducerProperties<KinesisProducerProperties>>
	implements LocalstackContainerTest {

	private static final String CLASS_UNDER_TEST_NAME = KinesisBinderTests.class
		.getSimpleName();

	private static AmazonKinesisAsync AMAZON_KINESIS;

	private static AmazonDynamoDBAsync DYNAMO_DB;


	public KinesisBinderTests() {
		this.timeoutMultiplier = 10D;
	}

	@BeforeAll
	public static void setup() {
		AMAZON_KINESIS = LocalstackContainerTest.kinesisClient();
		DYNAMO_DB = LocalstackContainerTest.dynamoDbClient();
		System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
	}

	@Test
	@Override
	public void testClean(TestInfo testInfo) {
	}

	@Test
	@Override
	public void testPartitionedModuleSpEL(TestInfo testInfo) {

	}

	@Test
	public void testAutoCreateStreamForNonExistingStream() throws Exception {
		KinesisTestBinder binder = getBinder();
		DirectChannel output = createBindableChannel("output", new BindingProperties());
		ExtendedConsumerProperties<KinesisConsumerProperties> consumerProperties = createConsumerProperties();
		Date testDate = new Date();
		consumerProperties.getExtension().setShardIteratorType(
			ShardIteratorType.AT_TIMESTAMP.name() + ":" + testDate.getTime());
		String testStreamName = "nonexisting" + System.currentTimeMillis();
		Binding<?> binding = binder.bindConsumer(testStreamName, "test", output,
			consumerProperties);
		binding.unbind();

		DescribeStreamResult streamResult = AMAZON_KINESIS.describeStream(testStreamName);
		String createdStreamName = streamResult.getStreamDescription().getStreamName();
		int createdShards = streamResult.getStreamDescription().getShards().size();
		String createdStreamStatus = streamResult.getStreamDescription()
			.getStreamStatus();

		assertThat(createdStreamName).isEqualTo(testStreamName);
		assertThat(createdShards).isEqualTo(consumerProperties.getInstanceCount()
			* consumerProperties.getConcurrency());
		assertThat(createdStreamStatus).isEqualTo(StreamStatus.ACTIVE.toString());

		KinesisShardOffset shardOffset = TestUtils.getPropertyValue(binding,
			"lifecycle.streamInitialSequence", KinesisShardOffset.class);
		assertThat(shardOffset.getIteratorType())
			.isEqualTo(ShardIteratorType.AT_TIMESTAMP);
		assertThat(shardOffset.getTimestamp()).isEqualTo(testDate);
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testAnonymousGroup(TestInfo testInfo) throws Exception {
		KinesisTestBinder binder = getBinder();
		ExtendedProducerProperties<KinesisProducerProperties> producerProperties = createProducerProperties();
		DirectChannel output = createBindableChannel("output",
			createProducerBindingProperties(producerProperties));

		Binding<MessageChannel> producerBinding = binder.bindProducer(
			String.format("defaultGroup%s0", getDestinationNameDelimiter()), output,
			producerProperties);

		ExtendedConsumerProperties<KinesisConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(0);

		QueueChannel input1 = new QueueChannel();
		Binding<MessageChannel> binding1 = binder.bindConsumer(
			String.format("defaultGroup%s0", getDestinationNameDelimiter()), null,
			input1, consumerProperties);

		consumerProperties.setInstanceIndex(1);

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer(
			String.format("defaultGroup%s0", getDestinationNameDelimiter()), null,
			input2, consumerProperties);

		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload1)
			.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
			.build());

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		binding2.unbind();

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload2)
			.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
			.build());

		binding2 = binder.bindConsumer(
			String.format("defaultGroup%s0", getDestinationNameDelimiter()), null,
			input2, consumerProperties);
		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload3)
			.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
			.build());

		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload2);
		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isNotNull();

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload2);

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload3);

		producerBinding.unbind();
		binding1.unbind();
		binding2.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testProducerErrorChannel() throws Exception {
		KinesisTestBinder binder = getBinder();

		final RuntimeException putRecordException = new RuntimeException(
			"putRecordRequestEx");
		final AtomicReference<Object> sent = new AtomicReference<>();
		AmazonKinesisAsync amazonKinesisMock = mock(AmazonKinesisAsync.class);
		BDDMockito
			.given(amazonKinesisMock.putRecordAsync(any(PutRecordRequest.class),
				any(AsyncHandler.class)))
			.willAnswer((Answer<Future<PutRecordResult>>) (invocation) -> {
				PutRecordRequest request = invocation.getArgument(0);
				sent.set(request.getData());
				AsyncHandler<?, ?> handler = invocation.getArgument(1);
				handler.onError(putRecordException);
				return mock(Future.class);
			});

		new DirectFieldAccessor(binder.getBinder()).setPropertyValue("amazonKinesis",
			amazonKinesisMock);

		ExtendedProducerProperties<KinesisProducerProperties> producerProps = createProducerProperties();
		producerProps.setErrorChannelEnabled(true);
		DirectChannel moduleOutputChannel = createBindableChannel("output",
			createProducerBindingProperties(producerProps));
		Binding<MessageChannel> producerBinding = binder.bindProducer("ec.0",
			moduleOutputChannel, producerProps);

		ApplicationContext applicationContext = TestUtils.getPropertyValue(
			binder.getBinder(), "applicationContext", ApplicationContext.class);
		SubscribableChannel ec = applicationContext.getBean("ec.0.errors",
			SubscribableChannel.class);
		final AtomicReference<Message<?>> errorMessage = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		ec.subscribe((message) -> {
			errorMessage.set(message);
			latch.countDown();
		});

		String messagePayload = "oops";
		moduleOutputChannel.send(new GenericMessage<>(messagePayload.getBytes()));

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorMessage.get()).isInstanceOf(ErrorMessage.class);
		assertThat(errorMessage.get().getPayload())
			.isInstanceOf(AwsRequestFailureException.class);
		AwsRequestFailureException exception = (AwsRequestFailureException) errorMessage
			.get().getPayload();
		assertThat(exception.getCause()).isSameAs(putRecordException);
		assertThat(((PutRecordRequest) exception.getRequest()).getData())
			.isSameAs(sent.get());
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBatchListener() throws Exception {
		KinesisTestBinder binder = getBinder();
		ExtendedProducerProperties<KinesisProducerProperties> producerProperties = createProducerProperties();
		DirectChannel output = createBindableChannel("output",
			createProducerBindingProperties(producerProperties));

		Binding<MessageChannel> outputBinding = binder.bindProducer("testBatchListener",
			output, producerProperties);

		for (int i = 0; i < 3; i++) {
			output.send(new GenericMessage<>(i));
		}

		ExtendedConsumerProperties<KinesisConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setListenerMode(ListenerMode.batch);
		consumerProperties.setUseNativeDecoding(true);

		QueueChannel input = new QueueChannel();
		Binding<MessageChannel> inputBinding = binder.bindConsumer("testBatchListener",
			null, input, consumerProperties);

		Message<List<?>> receivedMessage = (Message<List<?>>) receive(input);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload().size()).isEqualTo(3);

		receivedMessage.getPayload().forEach((r) -> {
			assertThat(r).isInstanceOf(Record.class);
		});

		outputBinding.unbind();
		inputBinding.unbind();
	}

	@Test
	@Disabled("Localstack doesn't support updateShardCount. Test only against real AWS Kinesis")
	public void testPartitionCountIncreasedIfAutoAddPartitionsSet() throws Exception {
		KinesisBinderConfigurationProperties configurationProperties = new KinesisBinderConfigurationProperties();

		String stream = "existing" + System.currentTimeMillis();

		AMAZON_KINESIS.createStream(stream, 1);

		List<Shard> shards = describeStream(stream);

		assertThat(shards.size()).isEqualTo(1);

		configurationProperties.setMinShardCount(6);
		configurationProperties.setAutoAddShards(true);
		KinesisTestBinder binder = getBinder(configurationProperties);

		ExtendedConsumerProperties<KinesisConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel output = createBindableChannel("output", new BindingProperties());
		Binding<?> binding = binder.bindConsumer(stream, "test", output, consumerProperties);
		binding.unbind();

		shards = describeStream(stream);

		assertThat(shards.size()).isEqualTo(6);
	}

	private List<Shard> describeStream(String stream) {
		String exclusiveStartShardId = null;

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
			.withStreamName(stream);

		List<Shard> shardList = new ArrayList<>();

		while (true) {
			DescribeStreamResult describeStreamResult;

			describeStreamRequest.withExclusiveStartShardId(exclusiveStartShardId);
			describeStreamResult = AMAZON_KINESIS.describeStream(describeStreamRequest);
			StreamDescription streamDescription = describeStreamResult
				.getStreamDescription();
			if (StreamStatus.ACTIVE.toString()
				.equals(streamDescription.getStreamStatus())) {
				shardList.addAll(streamDescription.getShards());

				if (streamDescription.getHasMoreShards()) {
					exclusiveStartShardId = shardList.get(shardList.size() - 1)
						.getShardId();
					continue;
				}
				else {
					return shardList;
				}
			}
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(ex);
			}
		}
	}

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	protected KinesisTestBinder getBinder() {
		return getBinder(new KinesisBinderConfigurationProperties());
	}

	private KinesisTestBinder getBinder(
		KinesisBinderConfigurationProperties kinesisBinderConfigurationProperties) {
		if (this.testBinder == null) {
			this.testBinder = new KinesisTestBinder(AMAZON_KINESIS, DYNAMO_DB, kinesisBinderConfigurationProperties);
			this.timeoutMultiplier = 20;
		}
		return this.testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<KinesisConsumerProperties> createConsumerProperties() {
		ExtendedConsumerProperties<KinesisConsumerProperties> kinesisConsumerProperties = new ExtendedConsumerProperties<>(
			new KinesisConsumerProperties());
		// set the default values that would normally be propagated by Spring Cloud Stream
		kinesisConsumerProperties.setInstanceCount(1);
		kinesisConsumerProperties.setInstanceIndex(0);
		kinesisConsumerProperties.getExtension().setShardIteratorType(ShardIteratorType.TRIM_HORIZON.name());
		return kinesisConsumerProperties;
	}

	private ExtendedProducerProperties<KinesisProducerProperties> createProducerProperties() {
		return this.createProducerProperties(null);
	}

	@Override
	protected ExtendedProducerProperties<KinesisProducerProperties> createProducerProperties(TestInfo testInto) {
		ExtendedProducerProperties<KinesisProducerProperties> producerProperties = new ExtendedProducerProperties<>(
			new KinesisProducerProperties());
		producerProperties.setPartitionKeyExpression(new LiteralExpression("1"));
		producerProperties.getExtension().setSync(true);
		return producerProperties;
	}

	@Override
	public Spy spyOn(String name) {
		throw new UnsupportedOperationException("'spyOn' is not used by Kinesis tests");
	}

}
