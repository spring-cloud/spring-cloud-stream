/*
 * Copyright 2014-2016 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.common.ErrorMapping;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import scala.collection.Seq;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.context.Lifecycle;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Binder} that uses Kafka as the underlying middleware.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 */
public class KafkaMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<KafkaConsumerProperties>,
				ExtendedProducerProperties<KafkaProducerProperties>, Collection<PartitionInfo>>
		implements ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties>,
		DisposableBean {

	private final KafkaBinderConfigurationProperties configurationProperties;

	private RetryOperations metadataRetryOperations;

	private final Map<String, Collection<PartitionInfo>> topicsInUse = new HashMap<>();

	private ProducerListener<byte[], byte[]> producerListener;

	private volatile Producer<byte[], byte[]> dlqProducer;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	public KafkaMessageChannelBinder(KafkaBinderConfigurationProperties configurationProperties) {
		super(false, headersToMap(configurationProperties));
		this.configurationProperties = configurationProperties;
	}

	private static String[] headersToMap(KafkaBinderConfigurationProperties configurationProperties) {
		String[] headersToMap;
		if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
			headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length + configurationProperties.getHeaders().length);
			System.arraycopy(configurationProperties.getHeaders(), 0, combinedHeadersToMap,
					BinderHeaders.STANDARD_HEADERS.length,
					configurationProperties.getHeaders().length);
			headersToMap = combinedHeadersToMap;
		}
		return headersToMap;
	}

	/**
	 * Retry configuration for operations such as validating topic creation
	 * @param metadataRetryOperations the retry configuration
	 */
	public void setMetadataRetryOperations(RetryOperations metadataRetryOperations) {
		this.metadataRetryOperations = metadataRetryOperations;
	}

	public void setExtendedBindingProperties(KafkaExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	@Override
	public void onInit() throws Exception {

		if (this.metadataRetryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(10);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(100);
			backOffPolicy.setMultiplier(2);
			backOffPolicy.setMaxInterval(1000);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			this.metadataRetryOperations = retryTemplate;
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.dlqProducer != null) {
			this.dlqProducer.close();
			this.dlqProducer = null;
		}
	}

	public void setProducerListener(ProducerListener<byte[], byte[]> producerListener) {
		this.producerListener = producerListener;
	}

	Map<String, Collection<PartitionInfo>> getTopicsInUse() {
		return this.topicsInUse;
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	protected MessageHandler createProducerMessageHandler(final String name,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) throws Exception {

		KafkaTopicUtils.validateTopicName(name);

		Collection<PartitionInfo> partitions = ensureTopicCreated(name, producerProperties.getPartitionCount());

		if (producerProperties.getPartitionCount() < partitions.size()) {
			if (this.logger.isInfoEnabled()) {
				this.logger.info("The `partitionCount` of the producer for topic " + name + " is "
						+ producerProperties.getPartitionCount() + ", smaller than the actual partition count of "
						+ partitions.size() + " of the topic. The larger number will be used instead.");
			}
		}

		this.topicsInUse.put(name, partitions);

		ProducerFactory<byte[], byte[]> producerFB = getProducerFactory(producerProperties);
		KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFB);
		if (this.producerListener != null) {
			kafkaTemplate.setProducerListener(this.producerListener);
		}
		return new ProducerConfigurationMessageHandler(kafkaTemplate, name, producerProperties);
	}

	@Override
	protected void createProducerDestinationIfNecessary(String name,
			ExtendedProducerProperties<KafkaProducerProperties> properties) {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Using kafka topic for outbound: " + name);
		}
		KafkaTopicUtils.validateTopicName(name);
		Collection<PartitionInfo> partitions = ensureTopicCreated(name, properties.getPartitionCount());
		if (properties.getPartitionCount() < partitions.size()) {
			if (this.logger.isInfoEnabled()) {
				this.logger.info("The `partitionCount` of the producer for topic " + name + " is "
						+ properties.getPartitionCount() + ", smaller than the actual partition count of "
						+ partitions.size() + " of the topic. The larger number will be used instead.");
			}
		}
		this.topicsInUse.put(name, partitions);
	}

	private ProducerFactory<byte[], byte[]> getProducerFactory(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(this.configurationProperties.getRequiredAcks()));
		props.put(ProducerConfig.LINGER_MS_CONFIG,
				String.valueOf(producerProperties.getExtension().getBatchTimeout()));
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
				producerProperties.getExtension().getCompressionType().toString());

		if (!ObjectUtils.isEmpty(producerProperties.getExtension().getConfiguration())) {
			props.putAll(producerProperties.getExtension().getConfiguration());
		}
		return new DefaultKafkaProducerFactory<>(props);
	}

	@Override
	protected Collection<PartitionInfo> createConsumerDestinationIfNecessary(String name, String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		KafkaTopicUtils.validateTopicName(name);
		if (properties.getInstanceCount() == 0) {
			throw new IllegalArgumentException("Instance count cannot be zero");
		}
		Collection<PartitionInfo> allPartitions = ensureTopicCreated(name,
				properties.getInstanceCount() * properties.getConcurrency());

		Collection<PartitionInfo> listenedPartitions;

		if (properties.getInstanceCount() == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (PartitionInfo partition : allPartitions) {
				// divide partitions across modules
				if ((partition.partition() % properties.getInstanceCount()) == properties.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		this.topicsInUse.put(name, listenedPartitions);
		return listenedPartitions;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected MessageProducer createConsumerEndpoint(String name, String group, Collection<PartitionInfo> destination,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !properties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;

		Map<String, Object> props = getConsumerConfig(anonymous, consumerGroup);
		Deserializer<byte[]> valueDecoder = new ByteArrayDeserializer();
		Deserializer<byte[]> keyDecoder = new ByteArrayDeserializer();

		if (!ObjectUtils.isEmpty(properties.getExtension().getConfiguration())) {
			props.putAll(properties.getExtension().getConfiguration());
		}

		ConsumerFactory<byte[], byte[]> consumerFactory = new DefaultKafkaConsumerFactory<>(props, keyDecoder,
				valueDecoder);

		Collection<PartitionInfo> listenedPartitions = destination;
		Assert.isTrue(!CollectionUtils.isEmpty(listenedPartitions), "A list of partitions must be provided");
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = getTopicPartitionInitialOffsets(
				listenedPartitions);

		final ContainerProperties containerProperties =
				anonymous || properties.getExtension().isAutoRebalanceEnabled() ? new ContainerProperties(name)
						: new ContainerProperties(topicPartitionInitialOffsets);

		int concurrency = Math.min(properties.getConcurrency(), listenedPartitions.size());
		final ConcurrentMessageListenerContainer<byte[], byte[]> messageListenerContainer =
				new ConcurrentMessageListenerContainer(
						consumerFactory, containerProperties) {

					@Override
					public void stop(Runnable callback) {
						super.stop(callback);
					}
				};
		messageListenerContainer.setConcurrency(concurrency);
		messageListenerContainer.getContainerProperties().setAckOnError(isAutoCommitOnError(properties));

		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}

		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}

		final KafkaMessageDrivenChannelAdapter<byte[], byte[]> kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter<>(
						messageListenerContainer);

		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		final RetryTemplate retryTemplate = buildRetryTemplate(properties);
		kafkaMessageDrivenChannelAdapter.setRetryTemplate(retryTemplate);

		if (properties.getExtension().isEnableDlq()) {
			final String dlqTopic = "error." + name + "." + group;
			initDlqProducer();
			messageListenerContainer.getContainerProperties().setErrorHandler(new ErrorHandler() {

				@Override
				public void handle(Exception thrownException, final ConsumerRecord message) {
					final byte[] key = message.key() != null ? Utils.toArray(ByteBuffer.wrap((byte[]) message.key()))
							: null;
					final byte[] payload = message.value() != null
							? Utils.toArray(ByteBuffer.wrap((byte[]) message.value())) : null;
					KafkaMessageChannelBinder.this.dlqProducer.send(new ProducerRecord<>(dlqTopic, key, payload),
							new Callback() {

								@Override
								public void onCompletion(RecordMetadata metadata, Exception exception) {
									StringBuffer messageLog = new StringBuffer();
									messageLog.append(" a message with key='"
											+ toDisplayString(ObjectUtils.nullSafeToString(key), 50) + "'");
									messageLog.append(" and payload='"
											+ toDisplayString(ObjectUtils.nullSafeToString(payload), 50) + "'");
									messageLog.append(" received from " + message.partition());
									if (exception != null) {
										KafkaMessageChannelBinder.this.logger.error(
												"Error sending to DLQ" + messageLog.toString(), exception);
									}
									else {
										if (KafkaMessageChannelBinder.this.logger.isDebugEnabled()) {
											KafkaMessageChannelBinder.this.logger.debug(
													"Sent to DLQ " + messageLog.toString());
										}
									}
								}
							});
				}
			});
		}
		return kafkaMessageDrivenChannelAdapter;
	}

	private Map<String, Object> getConsumerConfig(boolean anonymous, String consumerGroup) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				anonymous ? "latest" : "earliest");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		return props;
	}

	private boolean isAutoCommitOnError(ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		return properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: properties.getExtension().isAutoCommitOffset() && properties.getExtension().isEnableDlq();
	}

	private TopicPartitionInitialOffset[] getTopicPartitionInitialOffsets(
			Collection<PartitionInfo> listenedPartitions) {
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets =
				new TopicPartitionInitialOffset[listenedPartitions.size()];
		int i = 0;
		for (PartitionInfo partition : listenedPartitions) {

			topicPartitionInitialOffsets[i++] = new TopicPartitionInitialOffset(partition.topic(),
					partition.partition());
		}
		return topicPartitionInitialOffsets;
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the
	 * desired number.
	 */
	private Collection<PartitionInfo> ensureTopicCreated(final String topicName, final int partitionCount) {

		final ZkClient zkClient = new ZkClient(this.configurationProperties.getZkConnectionString(),
				this.configurationProperties.getZkSessionTimeout(),
				this.configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);
		try {
			final Properties topicConfig = new Properties();
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
			if (topicMetadata.errorCode() == ErrorMapping.NoError()) {
				// only consider minPartitionCount for resizing if autoAddPartitions is
				// true
				int effectivePartitionCount = this.configurationProperties.isAutoAddPartitions()
						? Math.max(this.configurationProperties.getMinPartitionCount(), partitionCount)
						: partitionCount;
				if (topicMetadata.partitionsMetadata().size() < effectivePartitionCount) {
					if (this.configurationProperties.isAutoAddPartitions()) {
						AdminUtils.addPartitions(zkUtils, topicName, effectivePartitionCount, null, false);
					}
					else {
						int topicSize = topicMetadata.partitionsMetadata().size();
						throw new BinderException("The number of expected partitions was: " + partitionCount + ", but "
								+ topicSize + (topicSize > 1 ? " have " : " has ") + "been found instead."
								+ "Consider either increasing the partition count of the topic or enabling " +
								"`autoAddPartitions`");
					}
				}
			}
			else if (topicMetadata.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
				if (this.configurationProperties.isAutoCreateTopics()) {
					Seq<Object> brokerList = zkUtils.getSortedBrokerList();
					// always consider minPartitionCount for topic creation
					int effectivePartitionCount = Math.max(this.configurationProperties.getMinPartitionCount(),
							partitionCount);
					final scala.collection.Map<Object, Seq<Object>> replicaAssignment = AdminUtils
							.assignReplicasToBrokers(brokerList, effectivePartitionCount,
									this.configurationProperties.getReplicationFactor(), -1, -1);
					this.metadataRetryOperations.execute(new RetryCallback<Object, RuntimeException>() {

						@Override
						public Object doWithRetry(RetryContext context) throws RuntimeException {
							AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topicName,
									replicaAssignment, topicConfig, true);
							return null;
						}
					});
				}
				else {
					throw new BinderException("Topic " + topicName + " does not exist");
				}
			}
			else {
				throw new BinderException("Error fetching Kafka topic metadata: ",
						ErrorMapping.exceptionFor(topicMetadata.errorCode()));
			}
			try {
				return this.metadataRetryOperations
						.execute(new RetryCallback<Collection<PartitionInfo>, Exception>() {

							@Override
							public Collection<PartitionInfo> doWithRetry(RetryContext context) throws Exception {
								Collection<PartitionInfo> partitions =
										getProducerFactory(
												new ExtendedProducerProperties<>(new KafkaProducerProperties()))
												.createProducer().partitionsFor(topicName);

								// do a sanity check on the partition set
								if (partitions.size() < partitionCount) {
									throw new IllegalStateException("The number of expected partitions was: "
											+ partitionCount + ", but " + partitions.size()
											+ (partitions.size() > 1 ? " have " : " has ") + "been found instead");
								}
								return partitions;
							}
						});
			}
			catch (Exception e) {
				this.logger.error("Cannot initialize Binder", e);
				throw new BinderException("Cannot initialize binder:", e);
			}

		}
		finally {
			zkClient.close();
		}
	}

	private synchronized void initDlqProducer() {
		try {
			if (this.dlqProducer == null) {
				synchronized (this) {
					if (this.dlqProducer == null) {
						// we can use the producer defaults as we do not need to tune
						// performance
						Map<String, Object> props = new HashMap<>();
						props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
								this.configurationProperties.getKafkaConnectionString());
						props.put(ProducerConfig.RETRIES_CONFIG, 0);
						props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
						props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
						props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
						props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
						props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
						DefaultKafkaProducerFactory<byte[], byte[]> defaultKafkaProducerFactory =
								new DefaultKafkaProducerFactory<>(props);
						this.dlqProducer = defaultKafkaProducerFactory.createProducer();
					}
				}
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot initialize DLQ producer:", e);
		}
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
	}

	private final class ProducerConfigurationMessageHandler extends KafkaProducerMessageHandler<byte[], byte[]>
			implements Lifecycle {

		private boolean running = true;

		private ProducerConfigurationMessageHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate, String topic,
				ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
			super(kafkaTemplate);
			setTopicExpression(new LiteralExpression(topic));
			setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
			if (producerProperties.isPartitioned()) {
				SpelExpressionParser parser = new SpelExpressionParser();
				setPartitionIdExpression(parser.parseExpression("headers.partition"));
			}
			if (producerProperties.getExtension().isSync()) {
				setSync(true);
			}
		}

		@Override
		public void start() {
			try {
				super.onInit();
			}
			catch (Exception e) {
				this.logger.error("Initialization errors: ", e);
				throw new RuntimeException(e);
			}
		}

		@Override
		public void stop() {
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}
	}
}
