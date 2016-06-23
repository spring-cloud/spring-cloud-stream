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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.api.TopicMetadata;
import kafka.common.ErrorMapping;
import kafka.serializer.DefaultDecoder;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.listener.AcknowledgingMessageListener;
import org.springframework.integration.kafka.listener.Acknowledgment;
import org.springframework.integration.kafka.listener.ErrorHandler;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.listener.KafkaNativeOffsetManager;
import org.springframework.integration.kafka.listener.MessageListener;
import org.springframework.integration.kafka.listener.OffsetManager;
import org.springframework.integration.kafka.support.KafkaProducerContext;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerFactoryBean;
import org.springframework.integration.kafka.support.ProducerListener;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
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
				ExtendedProducerProperties<KafkaProducerProperties>, Collection<Partition>>
		implements ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties>,
		DisposableBean {

	private static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

	private static final ThreadFactory DAEMON_THREAD_FACTORY;

	static {
		CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("kafka-binder-");
		threadFactory.setDaemon(true);
		DAEMON_THREAD_FACTORY = threadFactory;
	}

	private final KafkaBinderConfigurationProperties configurationProperties;

	private RetryOperations metadataRetryOperations;

	private final Map<String, Collection<Partition>> topicsInUse = new HashMap<>();

	// -------- Default values for properties -------

	private ConnectionFactory connectionFactory;

	private ProducerListener producerListener;

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

	ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	public void setProducerListener(ProducerListener producerListener) {
		this.producerListener = producerListener;
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
		ZookeeperConfiguration configuration = new ZookeeperConfiguration(
				new ZookeeperConnect(this.configurationProperties.getZkConnectionString()));
		configuration.setBufferSize(this.configurationProperties.getSocketBufferSize());
		configuration.setMaxWait(this.configurationProperties.getMaxWait());
		DefaultConnectionFactory defaultConnectionFactory = new DefaultConnectionFactory(configuration);
		defaultConnectionFactory.afterPropertiesSet();
		this.connectionFactory = defaultConnectionFactory;
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

	/**
	 * Allowed chars are ASCII alphanumerics, '.', '_' and '-'.
	 */
	static void validateTopicName(String topicName) {
		try {
			byte[] utf8 = topicName.getBytes("UTF-8");
			for (byte b : utf8) {
				if (!((b >= 'a') && (b <= 'z') || (b >= 'A') && (b <= 'Z') || (b >= '0') && (b <= '9') || (b == '.')
						|| (b == '-') || (b == '_'))) {
					throw new IllegalArgumentException(
							"Topic name can only have ASCII alphanumerics, '.', '_' and '-'");
				}
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new AssertionError(e); // Can't happen
		}
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	Map<String, Collection<Partition>> getTopicsInUse() {
		return this.topicsInUse;
	}

	@Override
	protected Collection<Partition> createConsumerDestinationIfNecessary(String name, String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		validateTopicName(name);
		if (properties.getInstanceCount() == 0) {
			throw new IllegalArgumentException("Instance count cannot be zero");
		}
		Collection<Partition> allPartitions = ensureTopicCreated(name,
				properties.getInstanceCount() * properties.getConcurrency());

		Collection<Partition> listenedPartitions;

		if (properties.getInstanceCount() == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (Partition partition : allPartitions) {
				// divide partitions across modules
				if ((partition.getId() % properties.getInstanceCount()) == properties.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		this.topicsInUse.put(name, listenedPartitions);
		return listenedPartitions;
	}


	@Override
	@SuppressWarnings("unchecked")
	protected MessageProducer createConsumerEndpoint(String name, String group, Collection<Partition> destination,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {

		Assert.isTrue(!CollectionUtils.isEmpty(destination), "A list of partitions must be provided");

		int concurrency = Math.min(properties.getConcurrency(), destination.size());

		final ExecutorService dispatcherTaskExecutor =
				Executors.newFixedThreadPool(concurrency, DAEMON_THREAD_FACTORY);
		final KafkaMessageListenerContainer messageListenerContainer = new KafkaMessageListenerContainer(
				this.connectionFactory, destination.toArray(new Partition[destination.size()])) {

			@Override
			public void stop(Runnable callback) {
				super.stop(callback);
				if (getOffsetManager() instanceof DisposableBean) {
					try {
						((DisposableBean) getOffsetManager()).destroy();
					}
					catch (Exception e) {
						KafkaMessageChannelBinder.this.logger.error("Error while closing the offset manager", e);
					}
				}
				dispatcherTaskExecutor.shutdown();
			}
		};

		if (this.logger.isDebugEnabled()) {
			this.logger.debug(
					"Listened partitions: " + StringUtils.collectionToCommaDelimitedString(destination));
		}

		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !properties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;

		long referencePoint = properties.getExtension().getStartOffset() != null
				? properties.getExtension().getStartOffset().getReferencePoint()
				: (anonymous ? OffsetRequest.LatestTime() : OffsetRequest.EarliestTime());
		OffsetManager offsetManager = createOffsetManager(consumerGroup, referencePoint);
		if (properties.getExtension().isResetOffsets()) {
			offsetManager.resetOffsets(destination);
		}
		messageListenerContainer.setOffsetManager(offsetManager);
		messageListenerContainer.setQueueSize(this.configurationProperties.getQueueSize());
		messageListenerContainer.setMaxFetch(this.configurationProperties.getFetchSize());
		boolean autoCommitOnError = properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: properties.getExtension().isAutoCommitOffset() && properties.getExtension().isEnableDlq();
		messageListenerContainer.setAutoCommitOnError(autoCommitOnError);
		messageListenerContainer.setRecoveryInterval(properties.getExtension().getRecoveryInterval());
		messageListenerContainer.setConcurrency(concurrency);
		messageListenerContainer.setDispatcherTaskExecutor(dispatcherTaskExecutor);
		final KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter(
				messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		kafkaMessageDrivenChannelAdapter.setKeyDecoder(new DefaultDecoder(null));
		kafkaMessageDrivenChannelAdapter.setPayloadDecoder(new DefaultDecoder(null));
		kafkaMessageDrivenChannelAdapter.setAutoCommitOffset(properties.getExtension().isAutoCommitOffset());
		kafkaMessageDrivenChannelAdapter.afterPropertiesSet();
		if (properties.getMaxAttempts() > 1) {
			// we need to wrap the adapter listener into a retrying listener so that the retry
			// logic is applied before the ErrorHandler is executed
			final RetryTemplate retryTemplate = buildRetryTemplate(properties);
			if (properties.getExtension().isAutoCommitOffset()) {
				final MessageListener originalMessageListener = (MessageListener) messageListenerContainer
						.getMessageListener();
				messageListenerContainer.setMessageListener(new MessageListener() {

					@Override
					public void onMessage(final KafkaMessage message) {
						try {
							retryTemplate.execute(new RetryCallback<Object, Throwable>() {

								@Override
								public Object doWithRetry(RetryContext context) {
									originalMessageListener.onMessage(message);
									return null;
								}
							});
						}
						catch (Throwable throwable) {
							if (throwable instanceof RuntimeException) {
								throw (RuntimeException) throwable;
							}
							else {
								throw new RuntimeException(throwable);
							}
						}
					}
				});
			}
			else {
				messageListenerContainer.setMessageListener(new AcknowledgingMessageListener() {

					final AcknowledgingMessageListener originalMessageListener =
							(AcknowledgingMessageListener) messageListenerContainer
									.getMessageListener();

					@Override
					public void onMessage(final KafkaMessage message, final Acknowledgment acknowledgment) {
						retryTemplate.execute(new RetryCallback<Object, RuntimeException>() {

							@Override
							public Object doWithRetry(RetryContext context) {
								originalMessageListener.onMessage(message, acknowledgment);
								return null;
							}
						});
					}
				});
			}
		}

		if (properties.getExtension().isEnableDlq()) {
			final String dlqTopic = "error." + name + "." + consumerGroup;
			initDlqProducer();
			messageListenerContainer.setErrorHandler(new ErrorHandler() {

				@Override
				public void handle(Exception thrownException, final KafkaMessage message) {
					final byte[] key = message.getMessage().key() != null ? Utils.toArray(message.getMessage().key())
							: null;
					final byte[] payload = message.getMessage().payload() != null
							? Utils.toArray(message.getMessage().payload()) : null;
					KafkaMessageChannelBinder.this.dlqProducer.send(new ProducerRecord<>(dlqTopic, key, payload),
							new Callback() {

								@Override
								public void onCompletion(RecordMetadata metadata, Exception exception) {
									StringBuffer messageLog = new StringBuffer();
									messageLog.append(" a message with key='"
											+ toDisplayString(ObjectUtils.nullSafeToString(key), 50) + "'");
									messageLog.append(" and payload='"
											+ toDisplayString(ObjectUtils.nullSafeToString(payload), 50) + "'");
									messageLog.append(" received from " + message.getMetadata().getPartition());
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

	@Override
	protected MessageHandler createProducerMessageHandler(final String destination,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) throws Exception {
		ProducerMetadata<byte[], byte[]> producerMetadata = new ProducerMetadata<>(destination, byte[].class,
				byte[].class,
				BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
		producerMetadata.setSync(producerProperties.getExtension().isSync());
		producerMetadata.setCompressionType(producerProperties.getExtension().getCompressionType());
		producerMetadata.setBatchBytes(producerProperties.getExtension().getBufferSize());
		Properties additional = new Properties();
		additional.put(ProducerConfig.ACKS_CONFIG, String.valueOf(this.configurationProperties.getRequiredAcks()));
		additional.put(ProducerConfig.LINGER_MS_CONFIG,
				String.valueOf(producerProperties.getExtension().getBatchTimeout()));
		ProducerFactoryBean<byte[], byte[]> producerFB = new ProducerFactoryBean<>(producerMetadata,
				this.configurationProperties.getKafkaConnectionString(), additional);
		final ProducerConfiguration<byte[], byte[]> producerConfiguration = new ProducerConfiguration<>(
				producerMetadata, producerFB.getObject());
		producerConfiguration.setProducerListener(this.producerListener);
		KafkaProducerContext kafkaProducerContext = new KafkaProducerContext();
		kafkaProducerContext.setProducerConfigurations(
				Collections.<String, ProducerConfiguration<?, ?>>singletonMap(destination, producerConfiguration));
		return new ProducerConfigurationMessageHandler(producerConfiguration, destination);
	}

	@Override
	protected void createProducerDestinationIfNecessary(String name,
			ExtendedProducerProperties<KafkaProducerProperties> properties) {
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Using kafka topic for outbound: " + name);
		}
		validateTopicName(name);
		Collection<Partition> partitions = ensureTopicCreated(name, properties.getPartitionCount());
		// If the topic already exists, and it has a larger number of partitions than the one set in `partitionCount`,
		// we will use the existing partition count of the topic instead of the user setting.
		if (properties.getPartitionCount() < partitions.size()) {
			if (this.logger.isInfoEnabled()) {
				this.logger.info("The `partitionCount` setting of the producer for topic " + name + " is "
						+ properties.getPartitionCount() + ", smaller than the actual partition count of "
						+ partitions.size() + " of the topic. The larger number will be used instead.");
			}
		}
		this.topicsInUse.put(name, partitions);
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the
	 * desired number. If a topic with a larger number of partitions already exists,
	 * the partition count remains unchanged.
	 */
	private Collection<Partition> ensureTopicCreated(final String topicName, final int partitionCount) {

		final ZkClient zkClient = new ZkClient(this.configurationProperties.getZkConnectionString(),
				this.configurationProperties.getZkSessionTimeout(),
				this.configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);
		try {
			final Properties topicConfig = new Properties();
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient);
			if (topicMetadata.errorCode() == ErrorMapping.NoError()) {
				// only consider minPartitionCount for resizing if autoAddPartitions is
				// true
				int effectivePartitionCount = this.configurationProperties.isAutoAddPartitions()
						? Math.max(this.configurationProperties.getMinPartitionCount(),
						partitionCount) : partitionCount;
				if (topicMetadata.partitionsMetadata().size() < effectivePartitionCount) {
					if (this.configurationProperties.isAutoAddPartitions()) {
						AdminUtils.addPartitions(zkClient, topicName, effectivePartitionCount, null, false,
								new Properties());
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
					Seq<Object> brokerList = ZkUtils.getSortedBrokerList(zkClient);
					// always consider minPartitionCount for topic creation
					int effectivePartitionCount = Math.max(this.configurationProperties.getMinPartitionCount(),
							partitionCount);
					final scala.collection.Map<Object, Seq<Object>> replicaAssignment = AdminUtils
							.assignReplicasToBrokers(brokerList, effectivePartitionCount,
									this.configurationProperties.getReplicationFactor(), -1, -1);
					this.metadataRetryOperations.execute(new RetryCallback<Object, RuntimeException>() {

						@Override
						public Object doWithRetry(RetryContext context) throws RuntimeException {
							AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topicName,
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
				Collection<Partition> partitions = this.metadataRetryOperations
						.execute(new RetryCallback<Collection<Partition>, Exception>() {

							@Override
							public Collection<Partition> doWithRetry(RetryContext context) throws Exception {
								KafkaMessageChannelBinder.this.connectionFactory.refreshMetadata(
										Collections.singleton(topicName));
								Collection<Partition> partitions =
										KafkaMessageChannelBinder.this.connectionFactory.getPartitions(topicName);
								// do a sanity check on the partition set
								if (partitions.size() < partitionCount) {
									throw new IllegalStateException("The number of expected partitions was: "
											+ partitionCount + ", but " + partitions.size()
											+ (partitions.size() > 1 ? " have " : " has ") + "been found instead");
								}
								KafkaMessageChannelBinder.this.connectionFactory.getLeaders(partitions);
								return partitions;
							}
						});
				return partitions;
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
						ProducerMetadata<byte[], byte[]> producerMetadata = new ProducerMetadata<>("dlqKafkaProducer",
								byte[].class, byte[].class, BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
						producerMetadata.setSync(false);
						producerMetadata.setCompressionType(ProducerMetadata.CompressionType.none);
						producerMetadata.setBatchBytes(16384);
						Properties additionalProps = new Properties();
						additionalProps.put(ProducerConfig.ACKS_CONFIG,
								String.valueOf(this.configurationProperties.getRequiredAcks()));
						additionalProps.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(0));
						ProducerFactoryBean<byte[], byte[]> producerFactoryBean = new ProducerFactoryBean<>(
								producerMetadata, this.configurationProperties.getKafkaConnectionString(),
								additionalProps);
						this.dlqProducer = producerFactoryBean.getObject();
					}
				}
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot initialize DLQ producer:", e);
		}
	}

	private OffsetManager createOffsetManager(String group, long referencePoint) {
		try {

			KafkaNativeOffsetManager kafkaOffsetManager = new KafkaNativeOffsetManager(this.connectionFactory,
					new ZookeeperConnect(this.configurationProperties.getZkConnectionString()),
					Collections.<Partition, Long>emptyMap());
			kafkaOffsetManager.setConsumerId(group);
			kafkaOffsetManager.setReferenceTimestamp(referencePoint);
			kafkaOffsetManager.afterPropertiesSet();

			WindowingOffsetManager windowingOffsetManager = new WindowingOffsetManager(kafkaOffsetManager);
			windowingOffsetManager.setTimespan(this.configurationProperties.getOffsetUpdateTimeWindow());
			windowingOffsetManager.setCount(this.configurationProperties.getOffsetUpdateCount());
			windowingOffsetManager.setShutdownTimeout(this.configurationProperties.getOffsetUpdateShutdownTimeout());

			windowingOffsetManager.afterPropertiesSet();
			return windowingOffsetManager;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
	}

	public enum StartOffset {
		earliest(OffsetRequest.EarliestTime()), latest(OffsetRequest.LatestTime());

		private final long referencePoint;

		StartOffset(long referencePoint) {
			this.referencePoint = referencePoint;
		}

		public long getReferencePoint() {
			return this.referencePoint;
		}
	}

	private final static class ProducerConfigurationMessageHandler implements MessageHandler, Lifecycle {

		private ProducerConfiguration<byte[], byte[]> delegate;

		private String targetTopic;

		private boolean running;

		private ProducerConfigurationMessageHandler(
				ProducerConfiguration<byte[], byte[]> delegate, String targetTopic) {
			Assert.notNull(delegate, "Delegate cannot be null");
			Assert.hasText(targetTopic, "Target topic cannot be null");
			this.delegate = delegate;
			this.targetTopic = targetTopic;

		}

		@Override
		public void start() {
			this.running = true;
		}

		@Override
		public void stop() {
			this.delegate.stop();
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			this.delegate.send(this.targetTopic,
					message.getHeaders().get(BinderHeaders.PARTITION_HEADER, Integer.class), null,
					(byte[]) message.getPayload());
		}
	}
}
