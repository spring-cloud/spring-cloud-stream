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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.api.TopicMetadata;
import kafka.common.ErrorMapping;
import kafka.serializer.Decoder;
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
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.FixedSubscriberChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
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
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerFactoryBean;
import org.springframework.integration.kafka.support.ProducerListener;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
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
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 */
public class KafkaMessageChannelBinder extends
		AbstractBinder<MessageChannel, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>>
		implements ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties>,
		DisposableBean {

	public static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

	public static final ThreadFactory DAEMON_THREAD_FACTORY;

	static {
		CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("kafka-binder-");
		threadFactory.setDaemon(true);
		DAEMON_THREAD_FACTORY = threadFactory;
	}

	private final KafkaBinderConfigurationProperties configurationProperties;

	private final String[] headersToMap;

	private RetryOperations metadataRetryOperations;

	private final Map<String, Collection<Partition>> topicsInUse = new HashMap<>();

	// -------- Default values for properties -------

	private ConnectionFactory connectionFactory;

	private ProducerListener producerListener;

	private volatile Producer<byte[], byte[]> dlqProducer;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	public KafkaMessageChannelBinder(KafkaBinderConfigurationProperties configurationProperties) {
		this.configurationProperties = configurationProperties;
		String[] configuredHeaders = configurationProperties.getHeaders();
		if (ObjectUtils.isEmpty(configuredHeaders)) {
			this.headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length + configuredHeaders.length);
			System.arraycopy(configuredHeaders, 0, combinedHeadersToMap, BinderHeaders.STANDARD_HEADERS.length,
					configuredHeaders.length);
			this.headersToMap = combinedHeadersToMap;
		}
	}

	String getZkAddress() {
		return this.configurationProperties.getZkConnectionString();
	}

	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
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
				new ZookeeperConnect(configurationProperties.getZkConnectionString()));
		configuration.setBufferSize(configurationProperties.getSocketBufferSize());
		configuration.setMaxWait(configurationProperties.getMaxWait());
		DefaultConnectionFactory defaultConnectionFactory = new DefaultConnectionFactory(configuration);
		defaultConnectionFactory.afterPropertiesSet();
		this.connectionFactory = defaultConnectionFactory;
		if (metadataRetryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(10);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(100);
			backOffPolicy.setMultiplier(2);
			backOffPolicy.setMaxInterval(1000);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			metadataRetryOperations = retryTemplate;
		}
	}

	@Override
	public void destroy() throws Exception {
		if (dlqProducer != null) {
			dlqProducer.close();
			dlqProducer = null;
		}
	}

	/**
	 * Allowed chars are ASCII alphanumerics, '.', '_' and '-'.
	 */
	public static void validateTopicName(String topicName) {
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
		return extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	Map<String, Collection<Partition>> getTopicsInUse() {
		return this.topicsInUse;
	}

	@Override
	protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		// If the caller provides a consumer group, use it; otherwise an anonymous
		// consumer group
		// is generated each time, such that each anonymous binding will receive all
		// messages.
		// Consumers reset offsets at the latest time by default, which allows them to
		// receive only
		// messages sent after they've been bound. That behavior can be changed with the
		// "resetOffsets" and "startOffset" properties.
		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !properties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
		// The reference point, if not set explicitly is the latest time for anonymous
		// subscriptions and the
		// earliest time for group subscriptions. This allows the latter to receive
		// messages published before the group
		// has been created.
		long referencePoint = properties.getExtension().getStartOffset() != null
				? properties.getExtension().getStartOffset().getReferencePoint()
				: (anonymous ? OffsetRequest.LatestTime() : OffsetRequest.EarliestTime());
		return createKafkaConsumer(name, inputChannel, properties, consumerGroup, referencePoint);
	}

	@Override
	public Binding<MessageChannel> doBindProducer(String name, MessageChannel moduleOutputChannel,
			ExtendedProducerProperties<KafkaProducerProperties> properties) {

		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		if (logger.isInfoEnabled()) {
			logger.info("Using kafka topic for outbound: " + name);
		}

		validateTopicName(name);

		Collection<Partition> partitions = ensureTopicCreated(name, properties.getPartitionCount());

		if (properties.getPartitionCount() < partitions.size()) {
			if (logger.isInfoEnabled()) {
				logger.info("The `partitionCount` of the producer for topic " + name + " is "
						+ properties.getPartitionCount() + ", smaller than the actual partition count of "
						+ partitions.size() + " of the topic. The larger number will be used instead.");
			}
		}

		topicsInUse.put(name, partitions);

		ProducerMetadata<byte[], byte[]> producerMetadata = new ProducerMetadata<>(name, byte[].class, byte[].class,
				BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
		producerMetadata.setSync(properties.getExtension().isSync());
		producerMetadata.setCompressionType(properties.getExtension().getCompressionType());
		producerMetadata.setBatchBytes(properties.getExtension().getBufferSize());
		Properties additionalProps = new Properties();
		additionalProps.put(ProducerConfig.ACKS_CONFIG, String.valueOf(configurationProperties.getRequiredAcks()));
		additionalProps.put(ProducerConfig.LINGER_MS_CONFIG,
				String.valueOf(properties.getExtension().getBatchTimeout()));
		ProducerFactoryBean<byte[], byte[]> producerFB = new ProducerFactoryBean<>(producerMetadata,
				configurationProperties.getKafkaConnectionString(), additionalProps);

		try {
			final ProducerConfiguration<byte[], byte[]> producerConfiguration = new ProducerConfiguration<>(
					producerMetadata, producerFB.getObject());
			producerConfiguration.setProducerListener(producerListener);

			MessageHandler handler = new SendingHandler(name, properties, partitions.size(), producerConfiguration);
			EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler) {

				@Override
				protected void doStop() {
					super.doStop();
					producerConfiguration.stop();
				}
			};
			consumer.setBeanFactory(this.getBeanFactory());
			consumer.setBeanName("outbound." + name);
			consumer.afterPropertiesSet();
			DefaultBinding<MessageChannel> producerBinding = new DefaultBinding<>(name, null, moduleOutputChannel,
					consumer);
			consumer.start();
			return producerBinding;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the
	 * desired number.
	 */
	private Collection<Partition> ensureTopicCreated(final String topicName, final int partitionCount) {

		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);
		try {
			final Properties topicConfig = new Properties();
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient);
			if (topicMetadata.errorCode() == ErrorMapping.NoError()) {
				// only consider minPartitionCount for resizing if autoAddPartitions is
				// true
				int effectivePartitionCount = configurationProperties.isAutoAddPartitions()
						? Math.max(configurationProperties.getMinPartitionCount(), partitionCount) : partitionCount;
				if (topicMetadata.partitionsMetadata().size() < effectivePartitionCount) {
					if (configurationProperties.isAutoAddPartitions()) {
						AdminUtils.addPartitions(zkClient, topicName, effectivePartitionCount, null, false,
								new Properties());
					}
					else {
						int topicSize = topicMetadata.partitionsMetadata().size();
						throw new BinderException("The number of expected partitions was: " + partitionCount + ", but "
								+ topicSize + (topicSize > 1 ? " have " : " has ") + "been found instead."
								+ "Consider either increasing the partition count of the topic or enabling `autoAddPartitions`");
					}
				}
			}
			else if (topicMetadata.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
				if (configurationProperties.isAutoCreateTopics()) {
					Seq<Object> brokerList = ZkUtils.getSortedBrokerList(zkClient);
					// always consider minPartitionCount for topic creation
					int effectivePartitionCount = Math.max(configurationProperties.getMinPartitionCount(),
							partitionCount);
					final scala.collection.Map<Object, Seq<Object>> replicaAssignment = AdminUtils
							.assignReplicasToBrokers(brokerList, effectivePartitionCount,
									configurationProperties.getReplicationFactor(), -1, -1);
					metadataRetryOperations.execute(new RetryCallback<Object, RuntimeException>() {
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
				Collection<Partition> partitions = metadataRetryOperations
						.execute(new RetryCallback<Collection<Partition>, Exception>() {

							@Override
							public Collection<Partition> doWithRetry(RetryContext context) throws Exception {
								connectionFactory.refreshMetadata(Collections.singleton(topicName));
								Collection<Partition> partitions = connectionFactory.getPartitions(topicName);
								// do a sanity check on the partition set
								if (partitions.size() < partitionCount) {
									throw new IllegalStateException("The number of expected partitions was: "
											+ partitionCount + ", but " + partitions.size()
											+ (partitions.size() > 1 ? " have " : " has ") + "been found instead");
								}
								connectionFactory.getLeaders(partitions);
								return partitions;
							}
						});
				return partitions;
			}
			catch (Exception e) {
				logger.error("Cannot initialize Binder", e);
				throw new BinderException("Cannot initialize binder:", e);
			}

		}
		finally {
			zkClient.close();
		}
	}

	private Binding<MessageChannel> createKafkaConsumer(String name, final MessageChannel moduleInputChannel,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties, String group, long referencePoint) {

		validateTopicName(name);
		if (properties.getInstanceCount() == 0) {
			throw new IllegalArgumentException("Instance count cannot be zero");
		}
		Collection<Partition> allPartitions = ensureTopicCreated(name,
				properties.getInstanceCount() * properties.getConcurrency());

		Decoder<byte[]> valueDecoder = new DefaultDecoder(null);
		Decoder<byte[]> keyDecoder = new DefaultDecoder(null);

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
		topicsInUse.put(name, listenedPartitions);
		ReceivingHandler rh = new ReceivingHandler(properties);
		rh.setOutputChannel(moduleInputChannel);

		final FixedSubscriberChannel bridge = new FixedSubscriberChannel(rh);
		bridge.setBeanName("bridge." + name);

		Assert.isTrue(!CollectionUtils.isEmpty(listenedPartitions), "A list of partitions must be provided");
		final KafkaMessageListenerContainer messageListenerContainer = new KafkaMessageListenerContainer(
				connectionFactory, listenedPartitions.toArray(new Partition[listenedPartitions.size()]));

		if (logger.isDebugEnabled()) {
			logger.debug("Listened partitions: " + StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}

		OffsetManager offsetManager = createOffsetManager(group, referencePoint);
		if (properties.getExtension().isResetOffsets()) {
			offsetManager.resetOffsets(listenedPartitions);
		}
		messageListenerContainer.setOffsetManager(offsetManager);
		messageListenerContainer.setQueueSize(configurationProperties.getQueueSize());
		messageListenerContainer.setMaxFetch(configurationProperties.getFetchSize());
		boolean autoCommitOnError = properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: properties.getExtension().isAutoCommitOffset() && properties.getExtension().isEnableDlq();
		messageListenerContainer.setAutoCommitOnError(autoCommitOnError);
		messageListenerContainer.setRecoveryInterval(properties.getExtension().getRecoveryInterval());

		int concurrency = Math.min(properties.getConcurrency(), listenedPartitions.size());
		messageListenerContainer.setConcurrency(concurrency);
		final ExecutorService dispatcherTaskExecutor = Executors.newFixedThreadPool(concurrency, DAEMON_THREAD_FACTORY);
		messageListenerContainer.setDispatcherTaskExecutor(dispatcherTaskExecutor);
		final KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter(
				messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		kafkaMessageDrivenChannelAdapter.setKeyDecoder(keyDecoder);
		kafkaMessageDrivenChannelAdapter.setPayloadDecoder(valueDecoder);
		kafkaMessageDrivenChannelAdapter.setOutputChannel(bridge);
		kafkaMessageDrivenChannelAdapter.setAutoCommitOffset(properties.getExtension().isAutoCommitOffset());
		kafkaMessageDrivenChannelAdapter.afterPropertiesSet();

		// we need to wrap the adapter listener into a retrying listener so that the retry
		// logic is applied before the ErrorHandler is executed
		final RetryTemplate retryTemplate = buildRetryTemplateIfRetryEnabled(properties);
		if (retryTemplate != null) {
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
					final AcknowledgingMessageListener originalMessageListener = (AcknowledgingMessageListener) messageListenerContainer
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
			final String dlqTopic = "error." + name + "." + group;
			initDlqProducer();
			messageListenerContainer.setErrorHandler(new ErrorHandler() {
				@Override
				public void handle(Exception thrownException, final KafkaMessage message) {
					final byte[] key = message.getMessage().key() != null ? Utils.toArray(message.getMessage().key())
							: null;
					final byte[] payload = message.getMessage().payload() != null
							? Utils.toArray(message.getMessage().payload()) : null;
					dlqProducer.send(new ProducerRecord<>(dlqTopic, key, payload), new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							StringBuffer messageLog = new StringBuffer();
							messageLog.append(" a message with key='"
									+ toDisplayString(ObjectUtils.nullSafeToString(key), 50) + "'");
							messageLog.append(" and payload='"
									+ toDisplayString(ObjectUtils.nullSafeToString(payload), 50) + "'");
							messageLog.append(" received from " + message.getMetadata().getPartition());
							if (exception != null) {
								logger.error("Error sending to DLQ" + messageLog.toString(), exception);
							}
							else {
								if (logger.isDebugEnabled()) {
									logger.debug("Sent to DLQ " + messageLog.toString());
								}
							}
						}
					});
				}
			});
		}
		kafkaMessageDrivenChannelAdapter.start();

		EventDrivenConsumer edc = new EventDrivenConsumer(bridge, rh) {

			@Override
			protected void doStop() {
				// stop the offset manager and the channel adapter before unbinding
				// this means that the upstream channel adapter has a chance to stop
				kafkaMessageDrivenChannelAdapter.stop();
				if (messageListenerContainer.getOffsetManager() instanceof DisposableBean) {
					try {
						((DisposableBean) messageListenerContainer.getOffsetManager()).destroy();
					}
					catch (Exception e) {
						logger.error("Error while closing the offset manager", e);
					}
				}
				super.doStop();
			}
		};
		String groupedName = groupedName(name, group);
		edc.setBeanName("inbound." + groupedName);

		DefaultBinding<MessageChannel> consumerBinding = new DefaultBinding<MessageChannel>(name, group,
				moduleInputChannel, edc) {
			@Override
			protected void afterUnbind() {
				dispatcherTaskExecutor.shutdown();
			}
		};
		edc.start();
		return consumerBinding;
	}

	private synchronized void initDlqProducer() {
		try {
			if (dlqProducer == null) {
				synchronized (this) {
					if (dlqProducer == null) {
						// we can use the producer defaults as we do not need to tune
						// performance
						ProducerMetadata<byte[], byte[]> producerMetadata = new ProducerMetadata<>("dlqKafkaProducer",
								byte[].class, byte[].class, BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
						producerMetadata.setSync(false);
						producerMetadata.setCompressionType(ProducerMetadata.CompressionType.none);
						producerMetadata.setBatchBytes(16384);
						Properties additionalProps = new Properties();
						additionalProps.put(ProducerConfig.ACKS_CONFIG,
								String.valueOf(configurationProperties.getRequiredAcks()));
						additionalProps.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(0));
						ProducerFactoryBean<byte[], byte[]> producerFactoryBean = new ProducerFactoryBean<>(
								producerMetadata, configurationProperties.getKafkaConnectionString(), additionalProps);
						dlqProducer = producerFactoryBean.getObject();
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

			KafkaNativeOffsetManager kafkaOffsetManager = new KafkaNativeOffsetManager(connectionFactory,
					new ZookeeperConnect(configurationProperties.getZkConnectionString()),
					Collections.<Partition, Long>emptyMap());
			kafkaOffsetManager.setConsumerId(group);
			kafkaOffsetManager.setReferenceTimestamp(referencePoint);
			kafkaOffsetManager.afterPropertiesSet();

			WindowingOffsetManager windowingOffsetManager = new WindowingOffsetManager(kafkaOffsetManager);
			windowingOffsetManager.setTimespan(configurationProperties.getOffsetUpdateTimeWindow());
			windowingOffsetManager.setCount(configurationProperties.getOffsetUpdateCount());
			windowingOffsetManager.setShutdownTimeout(configurationProperties.getOffsetUpdateShutdownTimeout());

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

	@Override
	public void doManualAck(LinkedList<MessageHeaders> messageHeadersList) {
		Iterator<MessageHeaders> iterator = messageHeadersList.iterator();
		while (iterator.hasNext()) {
			MessageHeaders headers = iterator.next();
			Acknowledgment acknowledgment = (Acknowledgment) headers.get(KafkaHeaders.ACKNOWLEDGMENT);
			Assert.notNull(acknowledgment,
					"Acknowledgement shouldn't be null when acknowledging kafka message " + "manually.");
			acknowledgment.acknowledge();
		}
	}

	private final class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		private final ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties;

		private ReceivingHandler(ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {
			this.consumerProperties = consumerProperties;
		}

		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			if (HeaderMode.embeddedHeaders.equals(consumerProperties.getHeaderMode())) {
				MessageValues messageValues = extractMessageValues(requestMessage);
				return MessageBuilder.createMessage(messageValues.getPayload(), new KafkaBinderHeaders(messageValues));
			}
			else {
				return requestMessage;
			}
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			// prevent the message from being copied again in superclass
			return false;
		}

		@SuppressWarnings("serial")
		private final class KafkaBinderHeaders extends MessageHeaders {
			KafkaBinderHeaders(Map<String, Object> headers) {
				super(headers, MessageHeaders.ID_VALUE_NONE, -1L);
			}
		}
	}

	private final class SendingHandler extends AbstractMessageHandler {

		private final AtomicInteger roundRobinCount = new AtomicInteger();

		private final String topicName;

		private final ExtendedProducerProperties<KafkaProducerProperties> producerProperties;

		private final int numberOfKafkaPartitions;

		private final ProducerConfiguration<byte[], byte[]> producerConfiguration;

		private final PartitionHandler partitionHandler;

		private SendingHandler(String topicName, ExtendedProducerProperties<KafkaProducerProperties> properties,
				int numberOfPartitions, ProducerConfiguration<byte[], byte[]> producerConfiguration) {
			this.topicName = topicName;
			producerProperties = properties;
			this.numberOfKafkaPartitions = numberOfPartitions;
			ConfigurableListableBeanFactory beanFactory = KafkaMessageChannelBinder.this.getBeanFactory();
			this.setBeanFactory(beanFactory);
			this.producerConfiguration = producerConfiguration;
			this.partitionHandler = new PartitionHandler(beanFactory, evaluationContext, partitionSelector, properties);
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			int targetPartition;
			if (producerProperties.isPartitioned()) {
				targetPartition = this.partitionHandler.determinePartition(message);
			}
			else {
				targetPartition = roundRobin() % numberOfKafkaPartitions;
			}
			if (HeaderMode.embeddedHeaders.equals(producerProperties.getHeaderMode())) {
				MessageValues transformed = serializePayloadIfNecessary(message);
				byte[] messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
						KafkaMessageChannelBinder.this.headersToMap);
				producerConfiguration.send(topicName, targetPartition, null, messageToSend);
			}
			else if (HeaderMode.raw.equals(producerProperties.getHeaderMode())) {
				Object contentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
				if (contentType != null && !contentType.equals(MediaType.APPLICATION_OCTET_STREAM_VALUE)) {
					logger.error("Raw mode supports only " + MediaType.APPLICATION_OCTET_STREAM_VALUE + " content type"
							+ message.getPayload().getClass());
				}
				if (message.getPayload() instanceof byte[]) {
					producerConfiguration.send(topicName, targetPartition, null, (byte[]) message.getPayload());
				}
				else {
					logger.error("Raw mode supports only byte[] payloads but value sent was of type "
							+ message.getPayload().getClass());
				}
			}
		}

		private int roundRobin() {
			int result = roundRobinCount.incrementAndGet();
			if (result == Integer.MAX_VALUE) {
				roundRobinCount.set(0);
			}
			return result;
		}

	}

	public enum StartOffset {
		earliest(OffsetRequest.EarliestTime()), latest(OffsetRequest.LatestTime());

		private final long referencePoint;

		StartOffset(long referencePoint) {
			this.referencePoint = referencePoint;
		}

		public long getReferencePoint() {
			return referencePoint;
		}
	}

}
