/*
 * Copyright 2014-2019 the original author or authors.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.EmbeddedHeaderUtils;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties.StandardHeaders;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer.PartitioningInterceptor;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.channel.ChannelInterceptorAware;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.support.TopicPartitionInitialOffset.SeekPosition;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * A {@link org.springframework.cloud.stream.binder.Binder} that uses Kafka as the
 * underlying middleware.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 * @author Henryk Konsek
 * @author Doug Saus
 */
public class KafkaMessageChannelBinder extends
		// @checkstyle:off
		AbstractMessageChannelBinder<ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>, KafkaTopicProvisioner>
		// @checkstyle:on
		implements
		ExtendedPropertiesBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties> {

	/**
	 * Kafka header for x-exception-fqcn.
	 */
	public static final String X_EXCEPTION_FQCN = "x-exception-fqcn";

	/**
	 * Kafka header for x-exception-stacktrace.
	 */
	public static final String X_EXCEPTION_STACKTRACE = "x-exception-stacktrace";

	/**
	 * Kafka header for x-exception-message.
	 */
	public static final String X_EXCEPTION_MESSAGE = "x-exception-message";

	/**
	 * Kafka header for x-original-topic.
	 */
	public static final String X_ORIGINAL_TOPIC = "x-original-topic";

	/**
	 * Kafka header for x-original-partition.
	 */
	public static final String X_ORIGINAL_PARTITION = "x-original-partition";

	/**
	 * Kafka header for x-original-offset.
	 */
	public static final String X_ORIGINAL_OFFSET = "x-original-offset";

	/**
	 * Kafka header for x-original-timestamp.
	 */
	public static final String X_ORIGINAL_TIMESTAMP = "x-original-timestamp";

	/**
	 * Kafka header for x-original-timestamp-type.
	 */
	public static final String X_ORIGINAL_TIMESTAMP_TYPE = "x-original-timestamp-type";

	private static final ThreadLocal<String> bindingNameHolder = new ThreadLocal<>();

	private final KafkaBinderConfigurationProperties configurationProperties;

	private final Map<String, TopicInformation> topicsInUse = new ConcurrentHashMap<>();

	private final KafkaTransactionManager<byte[], byte[]> transactionManager;

	private final KafkaBindingRebalanceListener rebalanceListener;

	private ProducerListener<byte[], byte[]> producerListener;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	public KafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider) {

		this(configurationProperties, provisioningProvider, null, null, null);
	}

	public KafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider,
			ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> containerCustomizer,
			KafkaBindingRebalanceListener rebalanceListener) {

		this(configurationProperties, provisioningProvider, containerCustomizer, null, rebalanceListener);
	}

	public KafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider,
			ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> containerCustomizer,
			MessageSourceCustomizer<KafkaMessageSource<?, ?>> sourceCustomizer,
			KafkaBindingRebalanceListener rebalanceListener) {

		super(headersToMap(configurationProperties), provisioningProvider,
				containerCustomizer, sourceCustomizer);
		this.configurationProperties = configurationProperties;
		if (StringUtils.hasText(
				configurationProperties.getTransaction().getTransactionIdPrefix())) {
			this.transactionManager = new KafkaTransactionManager<>(getProducerFactory(
					configurationProperties.getTransaction().getTransactionIdPrefix(),
					new ExtendedProducerProperties<>(configurationProperties
							.getTransaction().getProducer().getExtension())));
		}
		else {
			this.transactionManager = null;
		}
		this.rebalanceListener = rebalanceListener;
	}

	private static String[] headersToMap(
			KafkaBinderConfigurationProperties configurationProperties) {
		String[] headersToMap;
		if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
			headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(
					BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length
							+ configurationProperties.getHeaders().length);
			System.arraycopy(configurationProperties.getHeaders(), 0,
					combinedHeadersToMap, BinderHeaders.STANDARD_HEADERS.length,
					configurationProperties.getHeaders().length);
			headersToMap = combinedHeadersToMap;
		}
		return headersToMap;
	}

	public void setExtendedBindingProperties(
			KafkaExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	public void setProducerListener(ProducerListener<byte[], byte[]> producerListener) {
		this.producerListener = producerListener;
	}

	Map<String, TopicInformation> getTopicsInUse() {
		return this.topicsInUse;
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		bindingNameHolder.set(channelName);
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
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

	@Override
	protected MessageHandler createProducerMessageHandler(
			final ProducerDestination destination,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
			MessageChannel errorChannel) throws Exception {
		throw new IllegalStateException(
				"The abstract binder should not call this method");
	}

	@Override
	protected MessageHandler createProducerMessageHandler(
			final ProducerDestination destination,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
			MessageChannel channel, MessageChannel errorChannel) throws Exception {
		/*
		 * IMPORTANT: With a transactional binder, individual producer properties for
		 * Kafka are ignored; the global binder
		 * (spring.cloud.stream.kafka.binder.transaction.producer.*) properties are used
		 * instead, for all producers. A binder is transactional when
		 * 'spring.cloud.stream.kafka.binder.transaction.transaction-id-prefix' has text.
		 */
		final ProducerFactory<byte[], byte[]> producerFB = this.transactionManager != null
				? this.transactionManager.getProducerFactory()
				: getProducerFactory(null, producerProperties);
		Collection<PartitionInfo> partitions = provisioningProvider.getPartitionsForTopic(
				producerProperties.getPartitionCount(), false, () -> {
					Producer<byte[], byte[]> producer = producerFB.createProducer();
					List<PartitionInfo> partitionsFor = producer
							.partitionsFor(destination.getName());
					producer.close();
					((DisposableBean) producerFB).destroy();
					return partitionsFor;
				}, destination.getName());
		this.topicsInUse.put(destination.getName(),
				new TopicInformation(null, partitions, false));
		if (producerProperties.isPartitioned()
				&& producerProperties.getPartitionCount() < partitions.size()) {
			if (this.logger.isInfoEnabled()) {
				this.logger.info("The `partitionCount` of the producer for topic "
						+ destination.getName() + " is "
						+ producerProperties.getPartitionCount()
						+ ", smaller than the actual partition count of "
						+ partitions.size()
						+ " for the topic. The larger number will be used instead.");
			}
			List<ChannelInterceptor> interceptors = ((ChannelInterceptorAware) channel)
					.getChannelInterceptors();
			interceptors.forEach((interceptor) -> {
				if (interceptor instanceof PartitioningInterceptor) {
					((PartitioningInterceptor) interceptor)
							.setPartitionCount(partitions.size());
				}
			});
		}

		KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFB);
		if (this.producerListener != null) {
			kafkaTemplate.setProducerListener(this.producerListener);
		}
		ProducerConfigurationMessageHandler handler = new ProducerConfigurationMessageHandler(
				kafkaTemplate, destination.getName(), producerProperties, producerFB);
		if (errorChannel != null) {
			handler.setSendFailureChannel(errorChannel);
		}
		KafkaHeaderMapper mapper = null;
		if (this.configurationProperties.getHeaderMapperBeanName() != null) {
			mapper = getApplicationContext().getBean(
					this.configurationProperties.getHeaderMapperBeanName(),
					KafkaHeaderMapper.class);
		}
		/*
		 * Even if the user configures a bean, we must not use it if the header mode is
		 * not the default (headers); setting the mapper to null disables populating
		 * headers in the message handler.
		 */
		if (producerProperties.getHeaderMode() != null
				&& !HeaderMode.headers.equals(producerProperties.getHeaderMode())) {
			mapper = null;
		}
		else if (mapper == null) {
			String[] headerPatterns = producerProperties.getExtension()
					.getHeaderPatterns();
			if (headerPatterns != null && headerPatterns.length > 0) {
				List<String> patterns = new LinkedList<>(Arrays.asList(headerPatterns));
				if (!patterns.contains("!" + MessageHeaders.TIMESTAMP)) {
					patterns.add(0, "!" + MessageHeaders.TIMESTAMP);
				}
				if (!patterns.contains("!" + MessageHeaders.ID)) {
					patterns.add(0, "!" + MessageHeaders.ID);
				}
				mapper = new BinderHeaderMapper(
						patterns.toArray(new String[patterns.size()]));
			}
			else {
				mapper = new BinderHeaderMapper();
			}
		}
		handler.setHeaderMapper(mapper);
		return handler;
	}

	protected DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(
			String transactionIdPrefix,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				ByteArraySerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG,
				String.valueOf(this.configurationProperties.getRequiredAcks()));
		Map<String, Object> mergedConfig = this.configurationProperties
				.mergedProducerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.configurationProperties.getKafkaConnectionString());
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.BATCH_SIZE_CONFIG))) {
			props.put(ProducerConfig.BATCH_SIZE_CONFIG,
					String.valueOf(producerProperties.getExtension().getBufferSize()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.LINGER_MS_CONFIG))) {
			props.put(ProducerConfig.LINGER_MS_CONFIG,
					String.valueOf(producerProperties.getExtension().getBatchTimeout()));
		}
		if (ObjectUtils.isEmpty(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG))) {
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
					producerProperties.getExtension().getCompressionType().toString());
		}
		if (!ObjectUtils.isEmpty(producerProperties.getExtension().getConfiguration())) {
			props.putAll(producerProperties.getExtension().getConfiguration());
		}
		DefaultKafkaProducerFactory<byte[], byte[]> producerFactory = new DefaultKafkaProducerFactory<>(
				props);
		if (transactionIdPrefix != null) {
			producerFactory.setTransactionIdPrefix(transactionIdPrefix);
		}
		return producerFactory;
	}

	@Override
	protected boolean useNativeEncoding(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		if (this.transactionManager != null) {
			return this.configurationProperties.getTransaction().getProducer()
					.isUseNativeEncoding();
		}
		return super.useNativeEncoding(producerProperties);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected MessageProducer createConsumerEndpoint(
			final ConsumerDestination destination, final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(
				!anonymous || !extendedConsumerProperties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString()
				: group;
		final ConsumerFactory<?, ?> consumerFactory = createKafkaConsumerFactory(
				anonymous, consumerGroup, extendedConsumerProperties);
		int partitionCount = extendedConsumerProperties.getInstanceCount()
				* extendedConsumerProperties.getConcurrency();

		Collection<PartitionInfo> listenedPartitions = new ArrayList<>();

		boolean usingPatterns = extendedConsumerProperties.getExtension()
				.isDestinationIsPattern();
		Assert.isTrue(!usingPatterns || !extendedConsumerProperties.isMultiplex(),
				"Cannot use a pattern with multiplexed destinations; "
						+ "use the regex pattern to specify multiple topics instead");
		boolean groupManagement = extendedConsumerProperties.getExtension()
				.isAutoRebalanceEnabled();
		if (!extendedConsumerProperties.isMultiplex()) {
			listenedPartitions.addAll(processTopic(consumerGroup,
					extendedConsumerProperties, consumerFactory, partitionCount,
					usingPatterns, groupManagement, destination.getName()));
		}
		else {
			for (String name : StringUtils
					.commaDelimitedListToStringArray(destination.getName())) {
				listenedPartitions.addAll(processTopic(consumerGroup,
						extendedConsumerProperties, consumerFactory, partitionCount,
						usingPatterns, groupManagement, name.trim()));
			}
		}

		String[] topics = extendedConsumerProperties.isMultiplex()
				? StringUtils.commaDelimitedListToStringArray(destination.getName())
				: new String[] { destination.getName() };
		for (int i = 0; i < topics.length; i++) {
			topics[i] = topics[i].trim();
		}
		Assert.isTrue(usingPatterns || !CollectionUtils.isEmpty(listenedPartitions),
				"A list of partitions must be provided");
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = getTopicPartitionInitialOffsets(
				listenedPartitions);
		final ContainerProperties containerProperties = anonymous
				|| extendedConsumerProperties.getExtension().isAutoRebalanceEnabled()
						? usingPatterns
								? new ContainerProperties(Pattern.compile(topics[0]))
								: new ContainerProperties(topics)
						: new ContainerProperties(topicPartitionInitialOffsets);
		if (this.transactionManager != null) {
			containerProperties.setTransactionManager(this.transactionManager);
		}
		if (this.rebalanceListener != null) {
			setupRebalanceListener(extendedConsumerProperties, containerProperties);
		}
		containerProperties.setIdleEventInterval(
				extendedConsumerProperties.getExtension().getIdleEventInterval());
		int concurrency = usingPatterns ? extendedConsumerProperties.getConcurrency()
				: Math.min(extendedConsumerProperties.getConcurrency(),
						listenedPartitions.size());
		resetOffsets(extendedConsumerProperties, consumerFactory, groupManagement,
				containerProperties);
		@SuppressWarnings("rawtypes")
		final ConcurrentMessageListenerContainer<?, ?> messageListenerContainer = new ConcurrentMessageListenerContainer(
				consumerFactory, containerProperties) {

			@Override
			public void stop(Runnable callback) {
				super.stop(callback);
			}

		};
		messageListenerContainer.setConcurrency(concurrency);
		// these won't be needed if the container is made a bean
		if (getApplicationEventPublisher() != null) {
			messageListenerContainer
					.setApplicationEventPublisher(getApplicationEventPublisher());
		}
		else if (getApplicationContext() != null) {
			messageListenerContainer
					.setApplicationEventPublisher(getApplicationContext());
		}
		messageListenerContainer.setBeanName(topics + ".container");
		// end of these won't be needed...
		if (!extendedConsumerProperties.getExtension().isAutoCommitOffset()) {
			messageListenerContainer.getContainerProperties()
					.setAckMode(ContainerProperties.AckMode.MANUAL);
			messageListenerContainer.getContainerProperties().setAckOnError(false);
		}
		else {
			messageListenerContainer.getContainerProperties()
					.setAckOnError(isAutoCommitOnError(extendedConsumerProperties));
			if (extendedConsumerProperties.getExtension().isAckEachRecord()) {
				messageListenerContainer.getContainerProperties()
						.setAckMode(ContainerProperties.AckMode.RECORD);
			}
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Listened partitions: "
					+ StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}
		this.getContainerCustomizer().configure(messageListenerContainer,
				destination.getName(), group);
		// @checkstyle:off
		final KafkaMessageDrivenChannelAdapter<?, ?> kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter<>(
				messageListenerContainer);
		// @checkstyle:on
		kafkaMessageDrivenChannelAdapter
				.setMessageConverter(getMessageConverter(extendedConsumerProperties));
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination,
				consumerGroup, extendedConsumerProperties);
		if (extendedConsumerProperties.getMaxAttempts() > 1) {
			kafkaMessageDrivenChannelAdapter
					.setRetryTemplate(buildRetryTemplate(extendedConsumerProperties));
			kafkaMessageDrivenChannelAdapter
					.setRecoveryCallback(errorInfrastructure.getRecoverer());
		}
		else {
			kafkaMessageDrivenChannelAdapter
					.setErrorChannel(errorInfrastructure.getErrorChannel());
		}
		return kafkaMessageDrivenChannelAdapter;
	}

	public void setupRebalanceListener(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			final ContainerProperties containerProperties) {
		Assert.isTrue(!extendedConsumerProperties.getExtension().isResetOffsets(),
				"'resetOffsets' cannot be set when a KafkaBindingRebalanceListener is provided");
		final String bindingName = bindingNameHolder.get();
		bindingNameHolder.remove();
		Assert.notNull(bindingName, "'bindingName' cannot be null");
		final KafkaBindingRebalanceListener userRebalanceListener = this.rebalanceListener;
		containerProperties
				.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

					private boolean initial = true;

					@Override
					public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
							Collection<TopicPartition> partitions) {

						userRebalanceListener.onPartitionsRevokedBeforeCommit(bindingName,
								consumer, partitions);
					}

					@Override
					public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer,
							Collection<TopicPartition> partitions) {

						userRebalanceListener.onPartitionsRevokedAfterCommit(bindingName,
								consumer, partitions);
					}

					@Override
					public void onPartitionsAssigned(Consumer<?, ?> consumer,
							Collection<TopicPartition> partitions) {
						try {
							userRebalanceListener.onPartitionsAssigned(bindingName,
									consumer, partitions, this.initial);
						}
						finally {
							this.initial = false;
						}
					}

				});
	}

	public Collection<PartitionInfo> processTopic(final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			final ConsumerFactory<?, ?> consumerFactory, int partitionCount,
			boolean usingPatterns, boolean groupManagement, String topic) {
		Collection<PartitionInfo> listenedPartitions;
		Collection<PartitionInfo> allPartitions = usingPatterns ? Collections.emptyList()
				: getPartitionInfo(topic, extendedConsumerProperties, consumerFactory,
						partitionCount);

		if (groupManagement || extendedConsumerProperties.getInstanceCount() == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (PartitionInfo partition : allPartitions) {
				// divide partitions across modules
				if ((partition.partition() % extendedConsumerProperties
						.getInstanceCount()) == extendedConsumerProperties
								.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		this.topicsInUse.put(topic,
				new TopicInformation(group, listenedPartitions, usingPatterns));
		return listenedPartitions;
	}

	/*
	 * Reset the offsets if needed; may update the offsets in in the container's
	 * topicPartitionInitialOffsets.
	 */
	private void resetOffsets(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			final ConsumerFactory<?, ?> consumerFactory, boolean groupManagement,
			final ContainerProperties containerProperties) {

		boolean resetOffsets = extendedConsumerProperties.getExtension().isResetOffsets();
		final Object resetTo = consumerFactory.getConfigurationProperties()
				.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
		final AtomicBoolean initialAssignment = new AtomicBoolean(true);
		if (!"earliest".equals(resetTo) && !"latest".equals(resetTo)) {
			logger.warn("no (or unknown) " + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
					+ " property cannot reset");
			resetOffsets = false;
		}
		if (groupManagement && resetOffsets) {
			containerProperties
					.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

						@Override
						public void onPartitionsRevokedBeforeCommit(
								Consumer<?, ?> consumer, Collection<TopicPartition> tps) {
							// no op
						}

						@Override
						public void onPartitionsRevokedAfterCommit(
								Consumer<?, ?> consumer, Collection<TopicPartition> tps) {
							// no op
						}

						@Override
						public void onPartitionsAssigned(Consumer<?, ?> consumer,
								Collection<TopicPartition> tps) {
							if (initialAssignment.getAndSet(false)) {
								if ("earliest".equals(resetTo)) {
									consumer.seekToBeginning(tps);
								}
								else if ("latest".equals(resetTo)) {
									consumer.seekToEnd(tps);
								}
							}
						}
					});
		}
		else if (resetOffsets) {
			Arrays.stream(containerProperties.getTopicPartitions())
					.map(tpio -> new TopicPartitionInitialOffset(tpio.topic(),
							tpio.partition(),
							"earliest".equals(resetTo) ? SeekPosition.BEGINNING
									: SeekPosition.END))
					.collect(Collectors.toList())
					.toArray(containerProperties.getTopicPartitions());
		}
	}

	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name,
			String group, ConsumerDestination destination,
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {

		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(!anonymous || !consumerProperties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString()
				: group;
		final ConsumerFactory<?, ?> consumerFactory = createKafkaConsumerFactory(
				anonymous, consumerGroup, consumerProperties);
		String[] topics = consumerProperties.isMultiplex()
				? StringUtils.commaDelimitedListToStringArray(destination.getName())
				: new String[] { destination.getName() };
		for (int i = 0; i < topics.length; i++) {
			topics[i] = topics[i].trim();
		}
		KafkaMessageSource<?, ?> source = new KafkaMessageSource<>(consumerFactory,
				topics);
		source.setMessageConverter(getMessageConverter(consumerProperties));
		source.setRawMessageHeader(consumerProperties.getExtension().isEnableDlq());
		String clientId = name;
		if (consumerProperties.getExtension().getConfiguration()
				.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
			clientId = consumerProperties.getExtension().getConfiguration()
					.get(ConsumerConfig.CLIENT_ID_CONFIG);
		}
		source.setClientId(clientId);

		if (!consumerProperties.isMultiplex()) {
			// I copied this from the regular consumer - it looks bogus to me - includes
			// all partitions
			// not just the ones this binding is listening to; doesn't seem right for a
			// health check.
			Collection<PartitionInfo> partitionInfos = getPartitionInfo(
					destination.getName(), consumerProperties, consumerFactory, -1);
			this.topicsInUse.put(destination.getName(),
					new TopicInformation(consumerGroup, partitionInfos, false));
		}
		else {
			for (int i = 0; i < topics.length; i++) {
				Collection<PartitionInfo> partitionInfos = getPartitionInfo(topics[i],
						consumerProperties, consumerFactory, -1);
				this.topicsInUse.put(topics[i],
						new TopicInformation(consumerGroup, partitionInfos, false));
			}
		}

		source.setRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				KafkaMessageChannelBinder.this.logger.info("Revoked: " + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				KafkaMessageChannelBinder.this.logger.info("Assigned: " + partitions);
			}

		});
		getMessageSourceCustomizer().configure(source, destination.getName(), group);
		return new PolledConsumerResources(source, registerErrorInfrastructure(
				destination, group, consumerProperties, true));
	}

	@Override
	protected void postProcessPollableSource(DefaultPollableMessageSource bindingTarget) {
		bindingTarget.setAttributesProvider((accessor, message) -> {
			Object rawMessage = message.getHeaders().get(KafkaHeaders.RAW_DATA);
			if (rawMessage != null) {
				accessor.setAttribute(KafkaHeaders.RAW_DATA, rawMessage);
			}
		});
	}

	private MessagingMessageConverter getMessageConverter(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
		MessagingMessageConverter messageConverter;
		if (extendedConsumerProperties.getExtension().getConverterBeanName() == null) {
			messageConverter = new MessagingMessageConverter();
			StandardHeaders standardHeaders = extendedConsumerProperties.getExtension()
					.getStandardHeaders();
			messageConverter
					.setGenerateMessageId(StandardHeaders.id.equals(standardHeaders)
							|| StandardHeaders.both.equals(standardHeaders));
			messageConverter.setGenerateTimestamp(
					StandardHeaders.timestamp.equals(standardHeaders)
							|| StandardHeaders.both.equals(standardHeaders));
		}
		else {
			try {
				messageConverter = getApplicationContext().getBean(
						extendedConsumerProperties.getExtension().getConverterBeanName(),
						MessagingMessageConverter.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new IllegalStateException(
						"Converter bean not present in application context", ex);
			}
		}
		messageConverter.setHeaderMapper(getHeaderMapper(extendedConsumerProperties));
		return messageConverter;
	}

	private KafkaHeaderMapper getHeaderMapper(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
		KafkaHeaderMapper mapper = null;
		if (this.configurationProperties.getHeaderMapperBeanName() != null) {
			mapper = getApplicationContext().getBean(
					this.configurationProperties.getHeaderMapperBeanName(),
					KafkaHeaderMapper.class);
		}
		if (mapper == null) {
			BinderHeaderMapper headerMapper = new BinderHeaderMapper() {

				@Override
				public void toHeaders(Headers source, Map<String, Object> headers) {
					super.toHeaders(source, headers);
					if (headers.size() > 0) {
						headers.put(BinderHeaders.NATIVE_HEADERS_PRESENT, Boolean.TRUE);
					}
				}

			};
			String[] trustedPackages = extendedConsumerProperties.getExtension()
					.getTrustedPackages();
			if (!StringUtils.isEmpty(trustedPackages)) {
				headerMapper.addTrustedPackages(trustedPackages);
			}
			mapper = headerMapper;
		}
		return mapper;
	}

	private Collection<PartitionInfo> getPartitionInfo(String topic,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			final ConsumerFactory<?, ?> consumerFactory, int partitionCount) {
		return provisioningProvider.getPartitionsForTopic(partitionCount,
				extendedConsumerProperties.getExtension().isAutoRebalanceEnabled(),
				() -> {
					try (Consumer<?, ?> consumer = consumerFactory.createConsumer()) {
						List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
						return partitionsFor;
					}
				}, topic);
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return new RawRecordHeaderErrorMessageStrategy();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected MessageHandler getErrorMessageHandler(final ConsumerDestination destination,
			final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		KafkaConsumerProperties kafkaConsumerProperties = properties.getExtension();
		if (kafkaConsumerProperties.isEnableDlq()) {
			KafkaProducerProperties dlqProducerProperties = kafkaConsumerProperties
					.getDlqProducerProperties();
			ProducerFactory<?, ?> producerFactory = this.transactionManager != null
					? this.transactionManager.getProducerFactory()
					: getProducerFactory(null,
							new ExtendedProducerProperties<>(dlqProducerProperties));
			final KafkaTemplate<?, ?> kafkaTemplate = new KafkaTemplate<>(
					producerFactory);

			@SuppressWarnings("rawtypes")
			DlqSender<?, ?> dlqSender = new DlqSender(kafkaTemplate);

			return (message) -> {

				final ConsumerRecord<Object, Object> record = message.getHeaders()
						.get(KafkaHeaders.RAW_DATA, ConsumerRecord.class);

				if (properties.isUseNativeDecoding()) {
					if (record != null) {
						Map<String, String> configuration = this.transactionManager == null
								? dlqProducerProperties.getConfiguration()
								: this.configurationProperties.getTransaction()
										.getProducer().getConfiguration();
						if (record.key() != null
								&& !record.key().getClass().isInstance(byte[].class)) {
							ensureDlqMessageCanBeProperlySerialized(configuration,
									(Map<String, String> config) -> !config
											.containsKey("key.serializer"),
									"Key");
						}
						if (record.value() != null
								&& !record.value().getClass().isInstance(byte[].class)) {
							ensureDlqMessageCanBeProperlySerialized(configuration,
									(Map<String, String> config) -> !config
											.containsKey("value.serializer"),
									"Payload");
						}
					}
				}

				if (record == null) {
					this.logger.error("No raw record; cannot send to DLQ: " + message);
					return;
				}
				Headers kafkaHeaders = new RecordHeaders(record.headers().toArray());
				AtomicReference<ConsumerRecord<?, ?>> recordToSend = new AtomicReference<>(
						record);
				if (message.getPayload() instanceof Throwable) {

					Throwable throwable = (Throwable) message.getPayload();

					HeaderMode headerMode = properties.getHeaderMode();

					if (headerMode == null || HeaderMode.headers.equals(headerMode)) {

						kafkaHeaders.add(new RecordHeader(X_ORIGINAL_TOPIC,
								record.topic().getBytes(StandardCharsets.UTF_8)));
						kafkaHeaders.add(new RecordHeader(X_ORIGINAL_PARTITION,
								ByteBuffer.allocate(Integer.BYTES)
										.putInt(record.partition()).array()));
						kafkaHeaders.add(new RecordHeader(X_ORIGINAL_OFFSET, ByteBuffer
								.allocate(Long.BYTES).putLong(record.offset()).array()));
						kafkaHeaders.add(new RecordHeader(X_ORIGINAL_TIMESTAMP,
								ByteBuffer.allocate(Long.BYTES)
										.putLong(record.timestamp()).array()));
						kafkaHeaders.add(new RecordHeader(X_ORIGINAL_TIMESTAMP_TYPE,
								record.timestampType().toString()
										.getBytes(StandardCharsets.UTF_8)));
						kafkaHeaders.add(new RecordHeader(X_EXCEPTION_FQCN, throwable
								.getClass().getName().getBytes(StandardCharsets.UTF_8)));
						kafkaHeaders.add(new RecordHeader(X_EXCEPTION_MESSAGE,
								throwable.getMessage().getBytes(StandardCharsets.UTF_8)));
						kafkaHeaders.add(new RecordHeader(X_EXCEPTION_STACKTRACE,
								getStackTraceAsString(throwable)
										.getBytes(StandardCharsets.UTF_8)));
					}
					else if (HeaderMode.embeddedHeaders.equals(headerMode)) {
						try {
							MessageValues messageValues = EmbeddedHeaderUtils
									.extractHeaders(MessageBuilder
											.withPayload((byte[]) record.value()).build(),
											false);
							messageValues.put(X_ORIGINAL_TOPIC, record.topic());
							messageValues.put(X_ORIGINAL_PARTITION, record.partition());
							messageValues.put(X_ORIGINAL_OFFSET, record.offset());
							messageValues.put(X_ORIGINAL_TIMESTAMP, record.timestamp());
							messageValues.put(X_ORIGINAL_TIMESTAMP_TYPE,
									record.timestampType().toString());
							messageValues.put(X_EXCEPTION_FQCN,
									throwable.getClass().getName());
							messageValues.put(X_EXCEPTION_MESSAGE,
									throwable.getMessage());
							messageValues.put(X_EXCEPTION_STACKTRACE,
									getStackTraceAsString(throwable));

							final String[] headersToEmbed = new ArrayList<>(
									messageValues.keySet()).toArray(
											new String[messageValues.keySet().size()]);
							byte[] payload = EmbeddedHeaderUtils.embedHeaders(
									messageValues,
									EmbeddedHeaderUtils.headersToEmbed(headersToEmbed));
							recordToSend.set(new ConsumerRecord<Object, Object>(
									record.topic(), record.partition(), record.offset(),
									record.key(), payload));
						}
						catch (Exception ex) {
							throw new RuntimeException(ex);
						}
					}
				}
				String dlqName = StringUtils.hasText(kafkaConsumerProperties.getDlqName())
						? kafkaConsumerProperties.getDlqName()
						: "error." + record.topic() + "." + group;
				dlqSender.sendToDlq(recordToSend.get(), kafkaHeaders, dlqName);
			};
		}
		return null;
	}

	@Override
	protected MessageHandler getPolledConsumerErrorMessageHandler(
			ConsumerDestination destination, String group,
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		if (properties.getExtension().isEnableDlq()) {
			return getErrorMessageHandler(destination, group, properties);
		}
		final MessageHandler superHandler = super.getErrorMessageHandler(destination,
				group, properties);
		return (message) -> {
			ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) message.getHeaders()
					.get(KafkaHeaders.RAW_DATA);
			if (!(message instanceof ErrorMessage)) {
				logger.error("Expected an ErrorMessage, not a "
						+ message.getClass().toString() + " for: " + message);
			}
			else if (record == null) {
				if (superHandler != null) {
					superHandler.handleMessage(message);
				}
			}
			else {
				if (message.getPayload() instanceof MessagingException) {
					AcknowledgmentCallback ack = StaticMessageHeaderAccessor
							.getAcknowledgmentCallback(
									((MessagingException) message.getPayload())
											.getFailedMessage());
					if (ack != null) {
						if (isAutoCommitOnError(properties)) {
							ack.acknowledge(AcknowledgmentCallback.Status.REJECT);
						}
						else {
							ack.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
						}
					}
				}
			}
		};
	}

	private static void ensureDlqMessageCanBeProperlySerialized(
			Map<String, String> configuration,
			Predicate<Map<String, String>> configPredicate, String dataType) {
		if (CollectionUtils.isEmpty(configuration)
				|| configPredicate.test(configuration)) {
			throw new IllegalArgumentException("Native decoding is used on the consumer. "
					+ dataType
					+ " is not byte[] and no serializer is set on the DLQ producer.");
		}
	}

	protected ConsumerFactory<?, ?> createKafkaConsumerFactory(boolean anonymous,
			String consumerGroup,
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				anonymous ? "latest" : "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

		Map<String, Object> mergedConfig = this.configurationProperties
				.mergedConsumerConfiguration();
		if (!ObjectUtils.isEmpty(mergedConfig)) {
			props.putAll(mergedConfig);
		}
		if (ObjectUtils.isEmpty(props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.configurationProperties.getKafkaConnectionString());
		}
		if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getConfiguration())) {
			props.putAll(consumerProperties.getExtension().getConfiguration());
		}
		if (!ObjectUtils.isEmpty(consumerProperties.getExtension().getStartOffset())) {
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					consumerProperties.getExtension().getStartOffset().name());
		}

		return new DefaultKafkaConsumerFactory<>(props);
	}

	private boolean isAutoCommitOnError(
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		return properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: properties.getExtension().isAutoCommitOffset()
						&& properties.getExtension().isEnableDlq();
	}

	private TopicPartitionInitialOffset[] getTopicPartitionInitialOffsets(
			Collection<PartitionInfo> listenedPartitions) {
		final TopicPartitionInitialOffset[] topicPartitionInitialOffsets = new TopicPartitionInitialOffset[listenedPartitions
				.size()];
		int i = 0;
		for (PartitionInfo partition : listenedPartitions) {

			topicPartitionInitialOffsets[i++] = new TopicPartitionInitialOffset(
					partition.topic(), partition.partition());
		}
		return topicPartitionInitialOffsets;
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

	private final class ProducerConfigurationMessageHandler
			extends KafkaProducerMessageHandler<byte[], byte[]> implements Lifecycle {

		private boolean running = true;

		private final ProducerFactory<byte[], byte[]> producerFactory;

		ProducerConfigurationMessageHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate,
				String topic,
				ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
				ProducerFactory<byte[], byte[]> producerFactory) {
			super(kafkaTemplate);
			setTopicExpression(new LiteralExpression(topic));
			setMessageKeyExpression(
					producerProperties.getExtension().getMessageKeyExpression());
			setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
			if (producerProperties.isPartitioned()) {
				SpelExpressionParser parser = new SpelExpressionParser();
				setPartitionIdExpression(parser.parseExpression(
						"headers['" + BinderHeaders.PARTITION_HEADER + "']"));
			}
			if (producerProperties.getExtension().isSync()) {
				setSync(true);
			}
			this.producerFactory = producerFactory;
		}

		@Override
		public void start() {
			try {
				super.onInit();
			}
			catch (Exception ex) {
				this.logger.error("Initialization errors: ", ex);
				throw new RuntimeException(ex);
			}
		}

		@Override
		public void stop() {
			if (this.producerFactory instanceof Lifecycle) {
				((Lifecycle) producerFactory).stop();
			}
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

	}

	/**
	 * Inner class to capture topic details.
	 */
	static class TopicInformation {

		private final String consumerGroup;

		private final Collection<PartitionInfo> partitionInfos;

		private final boolean isTopicPattern;

		TopicInformation(String consumerGroup, Collection<PartitionInfo> partitionInfos,
				boolean isTopicPattern) {
			this.consumerGroup = consumerGroup;
			this.partitionInfos = partitionInfos;
			this.isTopicPattern = isTopicPattern;
		}

		String getConsumerGroup() {
			return this.consumerGroup;
		}

		boolean isConsumerTopic() {
			return this.consumerGroup != null;
		}

		boolean isTopicPattern() {
			return this.isTopicPattern;
		}

		Collection<PartitionInfo> getPartitionInfos() {
			return this.partitionInfos;
		}

	}

	/**
	 * Helper class to send to DLQ.
	 *
	 * @param <K> generic type for key
	 * @param <V> generic type for value
	 */
	private final class DlqSender<K, V> {

		private final KafkaTemplate<K, V> kafkaTemplate;

		DlqSender(KafkaTemplate<K, V> kafkaTemplate) {
			this.kafkaTemplate = kafkaTemplate;
		}

		@SuppressWarnings("unchecked")
		void sendToDlq(ConsumerRecord<?, ?> consumerRecord, Headers headers,
				String dlqName) {
			K key = (K) consumerRecord.key();
			V value = (V) consumerRecord.value();
			ProducerRecord<K, V> producerRecord = new ProducerRecord<>(dlqName,
					consumerRecord.partition(), key, value, headers);

			StringBuilder sb = new StringBuilder().append(" a message with key='")
					.append(toDisplayString(ObjectUtils.nullSafeToString(key), 50))
					.append("'").append(" and payload='")
					.append(toDisplayString(ObjectUtils.nullSafeToString(value), 50))
					.append("'").append(" received from ")
					.append(consumerRecord.partition());
			ListenableFuture<SendResult<K, V>> sentDlq = null;
			try {
				sentDlq = this.kafkaTemplate.send(producerRecord);
				sentDlq.addCallback(new ListenableFutureCallback<SendResult<K, V>>() {

					@Override
					public void onFailure(Throwable ex) {
						KafkaMessageChannelBinder.this.logger
								.error("Error sending to DLQ " + sb.toString(), ex);
					}

					@Override
					public void onSuccess(SendResult<K, V> result) {
						if (KafkaMessageChannelBinder.this.logger.isDebugEnabled()) {
							KafkaMessageChannelBinder.this.logger
									.debug("Sent to DLQ " + sb.toString());
						}
					}
				});
			}
			catch (Exception ex) {
				if (sentDlq == null) {
					KafkaMessageChannelBinder.this.logger
							.error("Error sending to DLQ " + sb.toString(), ex);
				}
			}

		}

	}

}
