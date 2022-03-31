/*
 * Copyright 2017-2020 the original author or authors.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.kinesis.adapter.SpringDynamoDBAdapterClient;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisConsumerProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kinesis.properties.KinesisProducerProperties;
import org.springframework.cloud.stream.binder.kinesis.provisioning.KinesisConsumerDestination;
import org.springframework.cloud.stream.binder.kinesis.provisioning.KinesisStreamProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.expression.EvaluationContext;
import org.springframework.integration.aws.inbound.kinesis.KclMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageHeaderErrorMessageStrategy;
import org.springframework.integration.aws.inbound.kinesis.KinesisShardOffset;
import org.springframework.integration.aws.outbound.AbstractAwsMessageHandler;
import org.springframework.integration.aws.outbound.KinesisMessageHandler;
import org.springframework.integration.aws.outbound.KplMessageHandler;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.InterceptableChannel;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 *
 * The Spring Cloud Stream Binder implementation for AWS Kinesis.
 *
 * @author Peter Oates
 * @author Artem Bilan
 * @author Arnaud Lecollaire
 * @author Dirk Bonhomme
 * @author Asiel Caballero
 * @author Dmytro Danilenkov
 *
 */
public class KinesisMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<KinesisConsumerProperties>,
				ExtendedProducerProperties<KinesisProducerProperties>, KinesisStreamProvisioner>
		implements
		ExtendedPropertiesBinder<MessageChannel, KinesisConsumerProperties, KinesisProducerProperties> {

	private static final ErrorMessageStrategy ERROR_MESSAGE_STRATEGY = new KinesisMessageHeaderErrorMessageStrategy();

	private final List<String> streamsInUse = new ArrayList<>();

	private final KinesisBinderConfigurationProperties configurationProperties;

	private final AmazonDynamoDBStreamsAdapterClient dynamoDBStreamsAdapter;

	private final AmazonKinesisAsync amazonKinesis;

	private final AWSCredentialsProvider awsCredentialsProvider;

	private final AmazonCloudWatch cloudWatchClient;

	private final AmazonDynamoDB dynamoDBClient;

	private KinesisExtendedBindingProperties extendedBindingProperties = new KinesisExtendedBindingProperties();

	@Nullable
	private ConcurrentMetadataStore checkpointStore;

	@Nullable
	private LockRegistry lockRegistry;

	@Nullable
	private KinesisProducerConfiguration kinesisProducerConfiguration;

	private EvaluationContext evaluationContext;

	private List<KinesisClientLibConfiguration> kinesisClientLibConfigurations;

	public KinesisMessageChannelBinder(KinesisBinderConfigurationProperties configurationProperties,
			KinesisStreamProvisioner provisioningProvider, AmazonKinesisAsync amazonKinesis,
			AWSCredentialsProvider awsCredentialsProvider,
			@Nullable AmazonDynamoDB dynamoDBClient,
			@Nullable AmazonDynamoDBStreams dynamoDBStreams,
			@Nullable AmazonCloudWatch cloudWatchClient) {

		super(headersToMap(configurationProperties), provisioningProvider);
		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null");
		Assert.notNull(awsCredentialsProvider, "'awsCredentialsProvider' must not be null");
		this.configurationProperties = configurationProperties;
		this.amazonKinesis = amazonKinesis;
		this.cloudWatchClient = cloudWatchClient;
		this.dynamoDBClient = dynamoDBClient;
		this.awsCredentialsProvider = awsCredentialsProvider;

		if (dynamoDBStreams != null) {
			this.dynamoDBStreamsAdapter = new SpringDynamoDBAdapterClient(dynamoDBStreams);
		}
		else {
			this.dynamoDBStreamsAdapter = null;
		}
	}

	public void setExtendedBindingProperties(KinesisExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	public void setCheckpointStore(ConcurrentMetadataStore checkpointStore) {
		this.checkpointStore = checkpointStore;
	}

	public void setLockRegistry(LockRegistry lockRegistry) {
		this.lockRegistry = lockRegistry;
	}

	public void setKinesisProducerConfiguration(KinesisProducerConfiguration kinesisProducerConfiguration) {
		this.kinesisProducerConfiguration = kinesisProducerConfiguration;
	}

	public void setKinesisClientLibConfigurations(List<KinesisClientLibConfiguration> kinesisClientLibConfigurations) {
		this.kinesisClientLibConfigurations = kinesisClientLibConfigurations;
	}

	@Override
	public KinesisConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KinesisProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	public AmazonKinesisAsync getAmazonKinesis() {
		return this.amazonKinesis;
	}

	public List<String> getStreamsInUse() {
		return this.streamsInUse;
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<KinesisProducerProperties> producerProperties,
			MessageChannel errorChannel) {

		FunctionExpression<Message<?>> partitionKeyExpression =
				new FunctionExpression<>((m) ->
						m.getHeaders().containsKey(BinderHeaders.PARTITION_HEADER)
								? m.getHeaders().get(BinderHeaders.PARTITION_HEADER)
								: m.getPayload().hashCode());
		final AbstractAwsMessageHandler<?> messageHandler;
		if (this.configurationProperties.isKplKclEnabled()) {
			messageHandler = createKplMessageHandler(destination, partitionKeyExpression);
		}
		else {
			messageHandler = createKinesisMessageHandler(destination, partitionKeyExpression);
		}
		messageHandler.setSync(producerProperties.getExtension().isSync());
		messageHandler.setSendTimeout(producerProperties.getExtension().getSendTimeout());
		messageHandler.setFailureChannel(errorChannel);
		messageHandler.setBeanFactory(getBeanFactory());

		this.streamsInUse.add(destination.getName());

		return messageHandler;
	}

	private AbstractAwsMessageHandler<?> createKinesisMessageHandler(ProducerDestination destination,
			FunctionExpression<Message<?>> partitionKeyExpression) {

		final KinesisMessageHandler messageHandler;
		messageHandler = new KinesisMessageHandler(this.amazonKinesis);
		messageHandler.setStream(destination.getName());
		messageHandler.setPartitionKeyExpression(partitionKeyExpression);
		return messageHandler;
	}

	private AbstractAwsMessageHandler<?> createKplMessageHandler(ProducerDestination destination,
			FunctionExpression<Message<?>> partitionKeyExpression) {

		final KplMessageHandler messageHandler;
		messageHandler = new KplMessageHandler(new KinesisProducer(this.kinesisProducerConfiguration));
		messageHandler.setStream(destination.getName());
		messageHandler.setPartitionKeyExpression(partitionKeyExpression);
		return messageHandler;
	}

	@Override
	protected void postProcessOutputChannel(MessageChannel outputChannel,
			ExtendedProducerProperties<KinesisProducerProperties> producerProperties) {

		if (outputChannel instanceof InterceptableChannel && producerProperties.isPartitioned()) {
			((InterceptableChannel) outputChannel)
					.addInterceptor(0,
							new ChannelInterceptor() {

								private final PartitionKeyExtractorStrategy partitionKeyExtractorStrategy;

								{
									if (StringUtils.hasText(producerProperties.getPartitionKeyExtractorName())) {
										this.partitionKeyExtractorStrategy =
												getBeanFactory()
														.getBean(producerProperties.getPartitionKeyExtractorName(),
																PartitionKeyExtractorStrategy.class);
									}
									else {
										this.partitionKeyExtractorStrategy =
												(message) ->
														producerProperties.getPartitionKeyExpression()
																.getValue(KinesisMessageChannelBinder.this.evaluationContext,
																		message);
									}
								}

								@Override
								public Message<?> preSend(Message<?> message, MessageChannel channel) {
									Object partitionKey = this.partitionKeyExtractorStrategy.extractKey(message);
									return MessageBuilder.fromMessage(message)
											.setHeader(BinderHeaders.PARTITION_OVERRIDE, partitionKey)
											.build();
								}

							});
		}
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination,
			String group,
			ExtendedConsumerProperties<KinesisConsumerProperties> properties) {

		ConsumerDestination destinationToUse = destination;

		if (properties.getExtension().isDynamoDbStreams()) {
			DescribeTableResult describeTableResult = this.dynamoDBClient.describeTable(destinationToUse.getName());
			String latestStreamArn = describeTableResult.getTable().getLatestStreamArn();
			if (StringUtils.hasText(latestStreamArn)) {
				destinationToUse = new KinesisConsumerDestination(latestStreamArn, Collections.emptyList());
			}
			else {
				throw new ProvisioningException("The DynamoDB table ["
						+ destinationToUse.getName()
						+ "] doesn't have Streams enabled.");
			}
		}
		else {
			this.streamsInUse.add(destinationToUse.getName());
		}

		MessageProducer adapter;
		if (this.configurationProperties.isKplKclEnabled()) {
			adapter = createKclConsumerEndpoint(destinationToUse, group, properties);
		}
		else {
			adapter = createKinesisConsumerEndpoint(destinationToUse, group, properties);
		}

		return adapter;
	}

	private MessageProducer createKclConsumerEndpoint(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<KinesisConsumerProperties> properties) {
		KinesisConsumerProperties kinesisConsumerProperties = properties.getExtension();

		if (kinesisConsumerProperties.getShardId() != null) {
			logger.warn("Kinesis Client Library doesn't does not support explicit shard configuration. " +
					"Ignoring 'shardId' property");
		}

		String shardIteratorType = kinesisConsumerProperties.getShardIteratorType();

		AmazonKinesis amazonKinesisClient =
				kinesisConsumerProperties.isDynamoDbStreams()
						? this.dynamoDBStreamsAdapter
						: this.amazonKinesis;

		String stream = destination.getName();

		KinesisClientLibConfiguration kinesisClientLibConfiguration = obtainKinesisClientLibConfiguration(stream, group);

		KclMessageDrivenChannelAdapter adapter;

		String consumerGroup;
		if (kinesisClientLibConfiguration == null) {
			adapter = new KclMessageDrivenChannelAdapter(stream, amazonKinesisClient, this.cloudWatchClient,
							this.dynamoDBClient, this.awsCredentialsProvider);
			boolean anonymous = !StringUtils.hasText(group);
			consumerGroup = anonymous ? "anonymous." + UUID.randomUUID() : group;
			adapter.setConsumerGroup(consumerGroup);
			if (StringUtils.hasText(shardIteratorType)) {
				adapter.setStreamInitialSequence(InitialPositionInStream.valueOf(shardIteratorType));
			}
			adapter.setIdleBetweenPolls(kinesisConsumerProperties.getIdleBetweenPolls());
			adapter.setConsumerBackoff(kinesisConsumerProperties.getConsumerBackoff());
			if (kinesisConsumerProperties.getWorkerId() != null) {
				adapter.setWorkerId(kinesisConsumerProperties.getWorkerId());
			}
		}
		else {
			adapter = new KclMessageDrivenChannelAdapter(kinesisClientLibConfiguration, amazonKinesisClient,
					this.cloudWatchClient, this.dynamoDBClient);
			consumerGroup = kinesisClientLibConfiguration.getApplicationName();
		}

		adapter.setCheckpointMode(kinesisConsumerProperties.getCheckpointMode());
		adapter.setCheckpointsInterval(kinesisConsumerProperties.getCheckpointInterval());

		adapter.setListenerMode(kinesisConsumerProperties.getListenerMode());

		if (properties.isUseNativeDecoding()) {
			adapter.setConverter(null);
		}
		else {
			// Defer byte[] conversion to the InboundContentTypeConvertingInterceptor
			adapter.setConverter((bytes) -> bytes);
		}

		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination, consumerGroup, properties);
		adapter.setErrorMessageStrategy(ERROR_MESSAGE_STRATEGY);
		adapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		adapter.setBindSourceRecord(true);
		return adapter;
	}

	private KinesisClientLibConfiguration obtainKinesisClientLibConfiguration(String stream, String group) {
		KinesisClientLibConfiguration candidate = null;
		for (KinesisClientLibConfiguration conf : this.kinesisClientLibConfigurations) {
			if (stream.equals(conf.getStreamName())) {
				candidate = conf;
				if (Objects.equals(group, conf.getApplicationName())) {
					break;
				}
			}
		}
		return candidate;
	}

	private MessageProducer createKinesisConsumerEndpoint(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<KinesisConsumerProperties> properties) {

		KinesisConsumerProperties kinesisConsumerProperties = properties.getExtension();

		if (properties.getInstanceCount() > 1 && properties.getExtension().getShardId() != null) {
			throw new InvalidArgumentException("'instanceCount' more than 1 and 'shardId' cannot be provided together.");
		}

		Set<KinesisShardOffset> shardOffsets = null;

		String shardIteratorType = kinesisConsumerProperties.getShardIteratorType();

		KinesisShardOffset kinesisShardOffset = KinesisShardOffset.latest();

		if (StringUtils.hasText(shardIteratorType)) {
			String[] typeValue = shardIteratorType.split(":", 2);
			ShardIteratorType iteratorType = ShardIteratorType.valueOf(typeValue[0]);
			kinesisShardOffset = new KinesisShardOffset(iteratorType);
			if (typeValue.length > 1) {
				if (ShardIteratorType.AT_TIMESTAMP.equals(iteratorType)) {
					kinesisShardOffset
							.setTimestamp(new Date(Long.parseLong(typeValue[1])));
				}
				else {
					kinesisShardOffset.setSequenceNumber(typeValue[1]);
				}
			}
		}

		if (properties.getInstanceCount() > 1) {
			shardOffsets = new HashSet<>();
			KinesisConsumerDestination kinesisConsumerDestination = (KinesisConsumerDestination) destination;
			List<Shard> shards = kinesisConsumerDestination.getShards();
			for (int i = 0; i < shards.size(); i++) {
				// divide shards across instances
				if ((i % properties.getInstanceCount()) == properties.getInstanceIndex()) {
					KinesisShardOffset shardOffset = new KinesisShardOffset(kinesisShardOffset);
					shardOffset.setStream(destination.getName());
					shardOffset.setShard(shards.get(i).getShardId());
					shardOffsets.add(shardOffset);
				}
			}
		}

		KinesisMessageDrivenChannelAdapter adapter;

		AmazonKinesis amazonKinesisClient =
				kinesisConsumerProperties.isDynamoDbStreams()
						? this.dynamoDBStreamsAdapter
						: this.amazonKinesis;

		String shardId = kinesisConsumerProperties.getShardId();

		if (CollectionUtils.isEmpty(shardOffsets) && shardId == null) {
			adapter = new KinesisMessageDrivenChannelAdapter(amazonKinesisClient, destination.getName());
		}
		else if (shardId != null) {
			KinesisShardOffset shardOffset = new KinesisShardOffset(kinesisShardOffset);
			shardOffset.setStream(destination.getName());
			shardOffset.setShard(shardId);
			adapter = new KinesisMessageDrivenChannelAdapter(amazonKinesisClient, shardOffset);
		}
		else {
			adapter = new KinesisMessageDrivenChannelAdapter(amazonKinesisClient,
					shardOffsets.toArray(new KinesisShardOffset[0]));
		}

		boolean anonymous = !StringUtils.hasText(group);
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID() : group;
		adapter.setConsumerGroup(consumerGroup);

		adapter.setStreamInitialSequence(
				anonymous || StringUtils.hasText(shardIteratorType) ? kinesisShardOffset
						: KinesisShardOffset.trimHorizon());

		adapter.setListenerMode(kinesisConsumerProperties.getListenerMode());

		if (properties.isUseNativeDecoding()) {
			adapter.setConverter(null);
		}
		else {
			// Defer byte[] conversion to the InboundContentTypeConvertingInterceptor
			adapter.setConverter((bytes) -> bytes);
		}

		adapter.setCheckpointMode(kinesisConsumerProperties.getCheckpointMode());
		adapter.setRecordsLimit(kinesisConsumerProperties.getRecordsLimit());
		adapter.setIdleBetweenPolls(kinesisConsumerProperties.getIdleBetweenPolls());
		adapter.setConsumerBackoff(kinesisConsumerProperties.getConsumerBackoff());
		adapter.setCheckpointsInterval(kinesisConsumerProperties.getCheckpointInterval());

		if (this.checkpointStore != null) {
			adapter.setCheckpointStore(this.checkpointStore);
		}

		adapter.setLockRegistry(this.lockRegistry);

		adapter.setConcurrency(properties.getConcurrency());
		adapter.setStartTimeout(kinesisConsumerProperties.getStartTimeout());
		adapter.setDescribeStreamBackoff(
				this.configurationProperties.getDescribeStreamBackoff());
		adapter.setDescribeStreamRetries(
				this.configurationProperties.getDescribeStreamRetries());

		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination,
				consumerGroup, properties);
		adapter.setErrorMessageStrategy(ERROR_MESSAGE_STRATEGY);
		adapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		adapter.setBindSourceRecord(true);
		return adapter;
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return ERROR_MESSAGE_STRATEGY;
	}

	private static String[] headersToMap(KinesisBinderConfigurationProperties configurationProperties) {
		Assert.notNull(configurationProperties,
				"'configurationProperties' must not be null");
		if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
			return BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(
					BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length
							+ configurationProperties.getHeaders().length);
			System.arraycopy(configurationProperties.getHeaders(), 0,
					combinedHeadersToMap, BinderHeaders.STANDARD_HEADERS.length,
					configurationProperties.getHeaders().length);
			return combinedHeadersToMap;
		}
	}

}
