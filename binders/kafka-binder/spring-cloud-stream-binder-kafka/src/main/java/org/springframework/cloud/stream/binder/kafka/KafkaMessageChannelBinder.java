/*
 * Copyright 2014-present the original author or authors.
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.cloud.stream.binder.kafka.config.ClientFactoryCustomizer;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.support.ConsumerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ProducerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.utils.BindingUtils;
import org.springframework.cloud.stream.binder.kafka.utils.DlqDestinationResolver;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
import org.springframework.cloud.stream.binding.DefaultPartitioningInterceptor;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer.PartitioningInterceptor;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter.ListenerMode;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.InterceptableChannel;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

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
 * @author Lukasz Kaminski
 * @author Taras Danylchuk
 * @author Yi Liu
 * @author Chris Bono
 * @author Byungjun You
 * @author Oliver Führer
 * @author Omer Celik
 * @author Didier Loiseau
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

	private static final Pattern interceptorNeededPattern = Pattern.compile("(payload|#root|#this)");

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final KafkaBinderConfigurationProperties configurationProperties;

	private final Map<String, TopicInformation> topicsInUse = new ConcurrentHashMap<>();

	private final KafkaTransactionManager<byte[], byte[]> transactionManager;

	private final TransactionTemplate transactionTemplate;

	private KafkaBindingRebalanceListener rebalanceListener;

	private DlqPartitionFunction dlqPartitionFunction;

	private DlqDestinationResolver dlqDestinationResolver;

	private final Map<ConsumerDestination, ContainerProperties.AckMode> ackModeInfo = new ConcurrentHashMap<>();

	private ProducerListener<byte[], byte[]> producerListener;

	private KafkaExtendedBindingProperties extendedBindingProperties = new KafkaExtendedBindingProperties();

	private final List<ClientFactoryCustomizer> clientFactoryCustomizers = new ArrayList<>();

	private final List<ProducerConfigCustomizer> producerConfigCustomizers = new ArrayList<>();

	private final List<ConsumerConfigCustomizer> consumerConfigCustomizers = new ArrayList<>();

	private final List<AbstractMessageListenerContainer<?, ?>> kafkaMessageListenerContainers = new ArrayList<>();

	private final KafkaAdmin kafkaAdmin;

	public KafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider) {

		this(configurationProperties, provisioningProvider, null, null, null,
				null, null);
	}

	public KafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider,
			ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> containerCustomizer,
			KafkaBindingRebalanceListener rebalanceListener) {

		this(configurationProperties, provisioningProvider, containerCustomizer, null, rebalanceListener,
				null, null);
	}

	public KafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider,
			ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> containerCustomizer,
			MessageSourceCustomizer<KafkaMessageSource<?, ?>> sourceCustomizer,
			KafkaBindingRebalanceListener rebalanceListener,
			DlqPartitionFunction dlqPartitionFunction,
			DlqDestinationResolver dlqDestinationResolver) {

		super(headersToMap(configurationProperties), provisioningProvider,
				containerCustomizer, sourceCustomizer);
		this.configurationProperties = configurationProperties;
		String txId = configurationProperties.getTransaction().getTransactionIdPrefix();
		if (StringUtils.hasText(txId)) {
			this.transactionManager = new KafkaTransactionManager<>(getProducerFactory(
					txId, new ExtendedProducerProperties<>(configurationProperties
							.getTransaction().getProducer().getExtension()), txId + ".producer", null));
			this.transactionTemplate = new TransactionTemplate(this.transactionManager);
		}
		else {
			this.transactionManager = null;
			this.transactionTemplate = null;
		}
		this.rebalanceListener = rebalanceListener;
		this.dlqPartitionFunction = dlqPartitionFunction;
		this.dlqDestinationResolver = dlqDestinationResolver;
		this.kafkaAdmin = new KafkaAdmin(new HashMap<>(provisioningProvider.getAdminClientProperties()));
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

	public void addClientFactoryCustomizer(ClientFactoryCustomizer customizer) {
		if (customizer != null) {
			this.clientFactoryCustomizers.add(customizer);
		}
	}

	public void setRebalanceListener(KafkaBindingRebalanceListener rebalanceListener) {
		this.rebalanceListener = rebalanceListener;
	}

	public void setDlqPartitionFunction(DlqPartitionFunction dlqPartitionFunction) {
		this.dlqPartitionFunction = dlqPartitionFunction;
	}

	public void setDlqDestinationResolver(DlqDestinationResolver dlqDestinationResolver) {
		this.dlqDestinationResolver = dlqDestinationResolver;
	}

	Map<String, TopicInformation> getTopicsInUse() {
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
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	/**
	 * Return a reference to the binder's transaction manager's producer factory (if
	 * configured). Use this to create a transaction manager in a bean definition when you
	 * wish to use producer-only transactions.
	 * @return the transaction manager, or null.
	 */
	@Nullable
	public ProducerFactory<byte[], byte[]> getTransactionalProducerFactory() {
		return this.transactionManager == null ? null : this.transactionManager.getProducerFactory();
	}

	@Override
	public String getBinderIdentity() {
		return "kafka-" + super.getBinderIdentity();
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
		 * Individual bindings can override the binder's transaction manager.
		 */
		KafkaAwareTransactionManager<byte[], byte[]> transMan = transactionManager(
				producerProperties.getExtension().getTransactionManager());
		final ProducerFactory<byte[], byte[]> producerFB = transMan != null
				? transMan.getProducerFactory()
				: getProducerFactory(null, producerProperties, destination.getName() + ".producer",
				destination.getName());
		Collection<PartitionInfo> partitions = provisioningProvider.getPartitionInfoForProducer(
				destination.getName(), producerFB, producerProperties);
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
			producerProperties.setPartitionCount(partitions.size());
			List<ChannelInterceptor> interceptors = ((InterceptableChannel) channel)
					.getInterceptors();
			interceptors.forEach((interceptor) -> {
				if (interceptor instanceof PartitioningInterceptor partitioningInterceptor) {
					partitioningInterceptor.setPartitionCount(partitions.size());
				}
				else if (interceptor instanceof DefaultPartitioningInterceptor defaultPartitioningInterceptor) {
					defaultPartitioningInterceptor.setPartitionCount(partitions.size());
				}
			});
		}

		KafkaTemplate<byte[], byte[]> kafkaTemplate = new KafkaTemplate<>(producerFB);
		if (this.producerListener != null) {
			kafkaTemplate.setProducerListener(this.producerListener);
		}
		if (transMan != null) {
			kafkaTemplate.setTransactionIdPrefix(configurationProperties.getTransaction().getTransactionIdPrefix());
		}
		final boolean allowNonTransactional = producerProperties.getExtension().isAllowNonTransactional();
		if (allowNonTransactional) {
			kafkaTemplate.setAllowNonTransactional(allowNonTransactional);
		}
		ProducerConfigurationMessageHandler handler = new ProducerConfigurationMessageHandler(
				kafkaTemplate, destination.getName(), producerProperties, producerFB, getBeanFactory());
		if (errorChannel != null) {
			handler.setSendFailureChannel(errorChannel);
		}
		if (StringUtils.hasText(producerProperties.getExtension().getRecordMetadataChannel())) {
			handler.setSendSuccessChannelName(producerProperties.getExtension().getRecordMetadataChannel());
		}
		AbstractApplicationContext applicationContext = getApplicationContext();
		handler.setApplicationContext(applicationContext);

		KafkaHeaderMapper mapper = BindingUtils.getHeaderMapper(applicationContext, this.configurationProperties);

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
			String[] headerPatterns = producerProperties.getExtension().getHeaderPatterns();
			if (headerPatterns != null && headerPatterns.length > 0) {
				mapper = new BinderHeaderMapper(
						BinderHeaderMapper.addNeverHeaderPatterns(Arrays.asList(headerPatterns)));
			}
			else {
				mapper = new BinderHeaderMapper();
			}
		}
		else {
			KafkaHeaderMapper userHeaderMapper = mapper;
			mapper = new KafkaHeaderMapper() {

				@Override
				public void toHeaders(Headers source, Map<String, Object> target) {
					userHeaderMapper.toHeaders(source, target);
				}

				@Override
				public void fromHeaders(MessageHeaders headers, Headers target) {
					userHeaderMapper.fromHeaders(headers, target);
					BinderHeaderMapper.removeNeverHeaders(target);
				}
			};

		}
		handler.setHeaderMapper(mapper);

		kafkaTemplate.setObservationEnabled(this.configurationProperties.isEnableObservation());
		kafkaTemplate.setKafkaAdmin(this.kafkaAdmin);
		kafkaTemplate.setApplicationContext(getApplicationContext());

		return handler;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected void customizeProducerMessageHandler(MessageHandler producerMessageHandler, String destinationName) {
		super.customizeProducerMessageHandler(producerMessageHandler, destinationName);
		((KafkaProducerMessageHandler) producerMessageHandler).getKafkaTemplate().afterSingletonsInstantiated();
	}

	@Override
	protected void postProcessOutputChannel(MessageChannel outputChannel,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {

		if (expressionInterceptorNeeded(producerProperties)) {
			((AbstractMessageChannel) outputChannel).addInterceptor(0, new KafkaExpressionEvaluatingInterceptor(
					producerProperties.getExtension().getMessageKeyExpression(), getEvaluationContext()));
		}
	}

	private boolean expressionInterceptorNeeded(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		if (producerProperties.isUseNativeEncoding()) {
			return false; // payload will be intact when it reaches the adapter
		}
		else {
			Expression messageKeyExpression = producerProperties.getExtension().getMessageKeyExpression();
			return messageKeyExpression != null
					&& interceptorNeededPattern.matcher(messageKeyExpression.getExpressionString()).find();
		}
	}

	protected DefaultKafkaProducerFactory<byte[], byte[]> getProducerFactory(
			String transactionIdPrefix,
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties, String beanName, String destination) {

		Map<String, Object> props = BindingUtils.createProducerConfigs(producerProperties,
				this.configurationProperties);
		final KafkaProducerProperties kafkaProducerProperties = producerProperties.getExtension();
		this.producerConfigCustomizers
			.forEach(customizer -> customizer.configure(props, producerProperties.getBindingName(), destination));
		DefaultKafkaProducerFactory<byte[], byte[]> producerFactory = new DefaultKafkaProducerFactory<>(
				props);
		if (transactionIdPrefix != null) {
			producerFactory.setTransactionIdPrefix(transactionIdPrefix);
		}
		if (kafkaProducerProperties.getCloseTimeout() > 0) {
			producerFactory.setPhysicalCloseTimeout(kafkaProducerProperties.getCloseTimeout());
		}
		producerFactory.setBeanName(beanName);
		this.clientFactoryCustomizers.forEach(customizer -> customizer.configure(producerFactory));
		return producerFactory;
	}

	@Override
	protected boolean useNativeEncoding(
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		if (transactionManager(producerProperties.getExtension().getTransactionManager()) != null) {
			return this.configurationProperties.getTransaction().getProducer()
					.isUseNativeEncoding();
		}
		return super.useNativeEncoding(producerProperties);
	}

	@Override
	protected MessageProducer createConsumerEndpoint(
			final ConsumerDestination destination, final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
		return createConsumerEndpointCaptureHelper(destination, group, extendedConsumerProperties);
	}

	@SuppressWarnings("unchecked")
	private <K, V> MessageProducer createConsumerEndpointCaptureHelper(
			final ConsumerDestination destination, final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

		boolean anonymous = !StringUtils.hasText(group);
		Assert.isTrue(
				!anonymous || !extendedConsumerProperties.getExtension().isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString()
				: group;
		final ConsumerFactory<K, V> consumerFactory = (ConsumerFactory<K, V>) createKafkaConsumerFactory(
				anonymous, consumerGroup, extendedConsumerProperties, destination.getName() + ".consumer", destination.getName());
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
			listenedPartitions.addAll(provisioningProvider.getListenedPartitions(consumerGroup,
					extendedConsumerProperties, consumerFactory, partitionCount,
					usingPatterns, groupManagement, destination.getName(), topicsInUse));
		}
		else {
			for (String name : StringUtils
					.commaDelimitedListToStringArray(destination.getName())) {
				listenedPartitions.addAll(provisioningProvider.getListenedPartitions(consumerGroup,
						extendedConsumerProperties, consumerFactory, partitionCount,
						usingPatterns, groupManagement, name.trim(), topicsInUse));
			}
		}

		String[] topics = extendedConsumerProperties.isMultiplex()
				? StringUtils.commaDelimitedListToStringArray(destination.getName())
				: new String[] { destination.getName() };
		for (int i = 0; i < topics.length; i++) {
			topics[i] = topics[i].trim();
		}
		if (!usingPatterns && !groupManagement) {
				Assert.isTrue(!CollectionUtils.isEmpty(listenedPartitions),
						"A list of partitions must be provided");
		}
		final TopicPartitionOffset[] topicPartitionOffsets = groupManagement
				? null
				: getTopicPartitionOffsets(listenedPartitions, extendedConsumerProperties, consumerFactory);
		final ContainerProperties containerProperties = anonymous
				|| groupManagement
						? usingPatterns
								? new ContainerProperties(Pattern.compile(topics[0]))
								: new ContainerProperties(topics)
						: new ContainerProperties(topicPartitionOffsets);

		containerProperties.setObservationEnabled(this.configurationProperties.isEnableObservation());

		KafkaAwareTransactionManager<byte[], byte[]> transMan = transactionManager(
				extendedConsumerProperties.getExtension().getTransactionManager());
		if (transMan != null) {
			containerProperties.setTransactionManager(transMan);
		}
		if (this.rebalanceListener != null) {
			setupRebalanceListener(extendedConsumerProperties, containerProperties);
		}
		containerProperties.setIdleEventInterval(
				extendedConsumerProperties.getExtension().getIdleEventInterval());
		int concurrency = usingPatterns ? extendedConsumerProperties.getConcurrency()
				: Math.min(extendedConsumerProperties.getConcurrency(),
						listenedPartitions.size());
		// in the event that auto rebalance is enabled, but no listened partitions are found
		// we want to make sure that concurrency is a non-zero value.
		if (groupManagement && listenedPartitions.isEmpty()) {
			concurrency = extendedConsumerProperties.getConcurrency();
		}
		resetOffsetsForAutoRebalance(extendedConsumerProperties, consumerFactory, containerProperties);
		containerProperties.setAuthExceptionRetryInterval(this.configurationProperties.getAuthorizationExceptionRetryInterval());
		final ConcurrentMessageListenerContainer<K, V> messageListenerContainer =
			new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties) {

			@Override
			public void stop(Runnable callback) {
				super.stop(callback);
			}

		};

		this.kafkaMessageListenerContainers.add(messageListenerContainer);
		messageListenerContainer.setKafkaAdmin(this.kafkaAdmin);
		messageListenerContainer.setConcurrency(concurrency);
		// these won't be needed if the container is made a bean
		AbstractApplicationContext applicationContext = getApplicationContext();
		messageListenerContainer.setApplicationContext(applicationContext);
		if (getApplicationEventPublisher() != null) {
			messageListenerContainer
					.setApplicationEventPublisher(getApplicationEventPublisher());
		}
		else if (applicationContext != null) {
			messageListenerContainer
					.setApplicationEventPublisher(applicationContext);
		}
		messageListenerContainer.setBeanName(destination + ".container");
		// end of these won't be needed...
		ContainerProperties.AckMode ackMode = extendedConsumerProperties.getExtension().getAckMode();

		if (ackMode != null) {
			if (!extendedConsumerProperties.isBatchMode() || ackMode != ContainerProperties.AckMode.RECORD) {
				messageListenerContainer.getContainerProperties()
						.setAckMode(ackMode);
			}
		}

		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Listened partitions: "
					+ StringUtils.collectionToCommaDelimitedString(listenedPartitions));
		}
		final KafkaMessageDrivenChannelAdapter<?, ?> kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter<>(messageListenerContainer,
						extendedConsumerProperties.isBatchMode() ? ListenerMode.batch : ListenerMode.record);
		MessageConverter messageConverter = getMessageConverter(extendedConsumerProperties);
		kafkaMessageDrivenChannelAdapter.setMessageConverter(messageConverter);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(getBeanFactory());
		kafkaMessageDrivenChannelAdapter.setApplicationContext(applicationContext);
		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(destination,
				consumerGroup, extendedConsumerProperties);

		ListenerContainerCustomizer<?> customizer = getContainerCustomizer();

		if (!extendedConsumerProperties.isBatchMode()
				&& extendedConsumerProperties.getMaxAttempts() > 1
				&& transMan == null) {
			if (!(customizer instanceof ListenerContainerWithDlqAndRetryCustomizer c)
					|| c.retryAndDlqInBinding(destination.getName(), group)) {
				kafkaMessageDrivenChannelAdapter
						.setRetryTemplate(buildRetryTemplate(extendedConsumerProperties));
				kafkaMessageDrivenChannelAdapter
						.setRecoveryCallback(errorInfrastructure.getRecoverer());
			}
			if (!extendedConsumerProperties.getExtension().isEnableDlq()) {
				messageListenerContainer.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
			}
		}
		else if (!extendedConsumerProperties.isBatchMode() && transMan != null) {
			var afterRollbackProcessor = new DefaultAfterRollbackProcessor<K, V>(
					(record, exception) -> {
						MessagingException payload =
								new MessagingException(((RecordMessageConverter) messageConverter)
											.toMessage(record, null, null, null),
										"Transaction rollback limit exceeded", exception);
						try {
							errorInfrastructure.getErrorChannel()
									.send(new ErrorMessage(
											payload,
												Collections.singletonMap(IntegrationMessageHeaderAccessor.SOURCE_DATA,
													record)));
						}
						catch (Exception e) {
							/*
							 * When there is no DLQ, the FinalRethrowingErrorMessageHandler will re-throw
							 * the payload; that will subvert the recovery and cause a re-seek of the failed
							 * record, so we ignore that here.
							 */
							if (!e.equals(payload)) {
								throw e;
							}
						}
					}, createBackOff(extendedConsumerProperties),
					new KafkaTemplate<>(transMan.getProducerFactory()),
					extendedConsumerProperties.getExtension().isTxCommitRecovered());
			if (!CollectionUtils.isEmpty(extendedConsumerProperties.getRetryableExceptions())) {
				// mimic AbstractBinder.buildRetryTemplate(properties)’s retryPolicy
				if (!extendedConsumerProperties.isDefaultRetryable()) {
					afterRollbackProcessor.defaultFalse(true);
				}
				extendedConsumerProperties.getRetryableExceptions()
						.forEach((t, retry) -> {
							if (Exception.class.isAssignableFrom(t)) {
								var ex = t.asSubclass(Exception.class);
								if (retry) {
									afterRollbackProcessor.addRetryableExceptions(ex);
								}
								else {
									afterRollbackProcessor.addNotRetryableExceptions(ex);
								}
							}
							else {
								throw new IllegalArgumentException(
										"Only Exception types can be configured as retryable-exceptions together with transactions. "
												+ "Unsupported type: " + t.getName());
							}
						});
			}
			messageListenerContainer.setAfterRollbackProcessor(afterRollbackProcessor);
		}
		else {
			kafkaMessageDrivenChannelAdapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		}
		final String commonErrorHandlerBeanName = extendedConsumerProperties.getExtension().getCommonErrorHandlerBeanName();
		if (StringUtils.hasText(commonErrorHandlerBeanName)) {
			final CommonErrorHandler commonErrorHandler = getApplicationContext().getBean(commonErrorHandlerBeanName,
					CommonErrorHandler.class);
			messageListenerContainer.setCommonErrorHandler(commonErrorHandler);
		}
		if (customizer instanceof ListenerContainerWithDlqAndRetryCustomizer c) {

			BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver = createDestResolver(
					extendedConsumerProperties.getExtension());
			BackOff createBackOff = extendedConsumerProperties.getMaxAttempts() > 1
					? createBackOff(extendedConsumerProperties)
					: null;
			c.configure(messageListenerContainer, destination.getName(), consumerGroup, destinationResolver,
							createBackOff, extendedConsumerProperties);
		}
		else if (customizer instanceof KafkaListenerContainerCustomizer c) {
			c.configure(messageListenerContainer, destination.getName(), consumerGroup, extendedConsumerProperties);
		}
		else {
			((ListenerContainerCustomizer<Object>) customizer)
					.configure(messageListenerContainer, destination.getName(), consumerGroup);
		}
		this.ackModeInfo.put(destination, messageListenerContainer.getContainerProperties().getAckMode());
		return kafkaMessageDrivenChannelAdapter;
	}

	private BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> createDestResolver(
			KafkaConsumerProperties extension) {

		Integer dlqPartitions = extension.getDlqPartitions();
		if (extension.isEnableDlq()) {
			return (rec, ex) -> dlqPartitions == null || dlqPartitions > 1
						? new TopicPartition(extension.getDlqName(), rec.partition())
						: new TopicPartition(extension.getDlqName(), 0);
		}
		else {
			return null;
		}
	}

	/**
	 * Configure a {@link BackOff} for the after rollback processor, based on the consumer
	 * retry properties. If retry is disabled, return a {@link BackOff} that disables
	 * retry. Otherwise use an {@link ExponentialBackOffWithMaxRetries}.
	 * @param extendedConsumerProperties the properties.
	 * @return the backoff.
	 */
	private BackOff createBackOff(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

		int maxAttempts = extendedConsumerProperties.getMaxAttempts();
		if (maxAttempts < 2) {
			return new FixedBackOff(0L, 0L);
		}
		int initialInterval = extendedConsumerProperties.getBackOffInitialInterval();
		int maxInterval = extendedConsumerProperties.getBackOffMaxInterval();
		double multiplier = extendedConsumerProperties.getBackOffMultiplier();
		ExponentialBackOff backOff = new ExponentialBackOffWithMaxRetries(maxAttempts - 1);
		backOff.setInitialInterval(initialInterval);
		backOff.setMaxInterval(maxInterval);
		backOff.setMultiplier(multiplier);
		return backOff;
	}

	public void setupRebalanceListener(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			final ContainerProperties containerProperties) {

		Assert.isTrue(!extendedConsumerProperties.getExtension().isResetOffsets(),
				"'resetOffsets' cannot be set when a KafkaBindingRebalanceListener is provided");
		final String bindingName = extendedConsumerProperties.getBindingName();
		Assert.notNull(bindingName, "'bindingName' cannot be null");
		final KafkaBindingRebalanceListener userRebalanceListener = this.rebalanceListener;
		containerProperties
				.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

					private final ThreadLocal<Boolean> initialAssignment = new ThreadLocal<>();

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
							Boolean initial = this.initialAssignment.get();
							if (initial == null) {
								initial = Boolean.TRUE;
							}
							userRebalanceListener.onPartitionsAssigned(bindingName,
									consumer, partitions, initial);
						}
						finally {
							this.initialAssignment.set(Boolean.FALSE);
						}
					}

				});
	}

	/*
	 * Reset the offsets if needed; may update the offsets in in the container's
	 * topicPartitionInitialOffsets.
	 */
	private void resetOffsetsForAutoRebalance(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			final ConsumerFactory<?, ?> consumerFactory, final ContainerProperties containerProperties) {

		final Object resetTo = checkReset(extendedConsumerProperties.getExtension().isResetOffsets(),
				consumerFactory.getConfigurationProperties()
				.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
		if (resetTo != null) {
			Set<TopicPartition> sought = ConcurrentHashMap.newKeySet();
			containerProperties.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

						@Override
						public void onPartitionsRevokedBeforeCommit(
								Consumer<?, ?> consumer, Collection<TopicPartition> tps) {

							if (logger.isInfoEnabled()) {
								logger.info("Partitions revoked: " + tps);
							}
						}

						@Override
						public void onPartitionsRevokedAfterCommit(
								Consumer<?, ?> consumer, Collection<TopicPartition> tps) {
							// no op
						}

						@Override
						public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> tps) {
							if (logger.isInfoEnabled()) {
								logger.info("Partitions assigned: " + tps);
							}
							List<TopicPartition> toSeek = tps.stream()
								.filter(tp -> {
									boolean shouldSeek = !sought.contains(tp);
									sought.add(tp);
									return shouldSeek;
								})
								.collect(Collectors.toList());
							if (toSeek.size() > 0) {
								if ("earliest".equals(resetTo)) {
									consumer.seekToBeginning(toSeek);
								}
								else if ("latest".equals(resetTo)) {
									consumer.seekToEnd(toSeek);
								}
							}
						}
					});
		}
	}

	private Object checkReset(boolean resetOffsets, final Object resetTo) {
		if (!resetOffsets) {
			return null;
		}
		else if (!"earliest".equals(resetTo) && !"latest".equals(resetTo)) {
			logger.warn("no (or unknown) " + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
					+ " property cannot reset");
			return null;
		}
		else {
			return resetTo;
		}
	}

	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name,
			String group, ConsumerDestination destination,
			ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

		boolean anonymous = !StringUtils.hasText(group);
		final KafkaConsumerProperties extension = extendedConsumerProperties.getExtension();
		Assert.isTrue(!anonymous || !extension.isEnableDlq(),
				"DLQ support is not available for anonymous subscriptions");
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString()
				: group;
		final ConsumerFactory<?, ?> consumerFactory = createKafkaConsumerFactory(
				anonymous, consumerGroup, extendedConsumerProperties, destination.getName() + ".polled.consumer",
				destination.getName());
		String[] topics = extendedConsumerProperties.isMultiplex()
				? StringUtils.commaDelimitedListToStringArray(destination.getName())
				: new String[] { destination.getName() };
		for (int i = 0; i < topics.length; i++) {
			topics[i] = topics[i].trim();
		}
		final ConsumerProperties consumerProperties = new ConsumerProperties(topics);

		String clientId = name;
		if (extension.getConfiguration()
				.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
			clientId = extension.getConfiguration()
					.get(ConsumerConfig.CLIENT_ID_CONFIG);
		}

		consumerProperties.setClientId(clientId);

		consumerProperties.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				KafkaMessageChannelBinder.this.logger.info("Revoked: " + partitions);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				KafkaMessageChannelBinder.this.logger.info("Assigned: " + partitions);
			}

		});

		consumerProperties.setPollTimeout(extension.getPollTimeout());

		KafkaMessageSource<?, ?> source = new KafkaMessageSource<>(consumerFactory,
				consumerProperties);
		MessageConverter messageConverter = getMessageConverter(extendedConsumerProperties);
		Assert.isInstanceOf(RecordMessageConverter.class, messageConverter,
				"'messageConverter' must be a 'RecordMessageConverter' for polled consumers");
		source.setMessageConverter((RecordMessageConverter) messageConverter);
		source.setRawMessageHeader(extension.isEnableDlq());

		if (!extendedConsumerProperties.isMultiplex()) {
			// I copied this from the regular consumer - it looks bogus to me - includes
			// all partitions
			// not just the ones this binding is listening to; doesn't seem right for a
			// health check.
			Collection<PartitionInfo> partitionInfos = provisioningProvider.getPartitionInfoForConsumer(
					destination.getName(), extendedConsumerProperties, consumerFactory, -1);
			this.topicsInUse.put(destination.getName(),
					new TopicInformation(consumerGroup, partitionInfos, false));
		}
		else {
			for (int i = 0; i < topics.length; i++) {
				Collection<PartitionInfo> partitionInfos = provisioningProvider.getPartitionInfoForConsumer(topics[i],
						extendedConsumerProperties, consumerFactory, -1);
				this.topicsInUse.put(topics[i],
						new TopicInformation(consumerGroup, partitionInfos, false));
			}
		}

		getMessageSourceCustomizer().configure(source, destination.getName(), group);
		return new PolledConsumerResources(source, registerErrorInfrastructure(
				destination, group, extendedConsumerProperties, true));
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

	private MessageConverter getMessageConverter(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

		MessageConverter messageConverter = BindingUtils.getConsumerMessageConverter(getApplicationContext(),
				extendedConsumerProperties, this.configurationProperties);
		if (messageConverter instanceof MessagingMessageConverter messagingMessageConverter) {
			messagingMessageConverter.setHeaderMapper(getHeaderMapper(extendedConsumerProperties));
		}
		return messageConverter;
	}

	private KafkaHeaderMapper getHeaderMapper(
			final ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {

		KafkaHeaderMapper mapper = BindingUtils.getHeaderMapper(getApplicationContext(), this.configurationProperties);
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
			if (!ObjectUtils.isEmpty(trustedPackages)) {
				headerMapper.addTrustedPackages(trustedPackages);
			}
			mapper = headerMapper;
		}
		return mapper;
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return new RawRecordHeaderErrorMessageStrategy();
	}

	private Boolean isBatchAndListenerContainerWithDlqAndRetryCustomizer(ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		ListenerContainerCustomizer<?> customizer = getContainerCustomizer();
		return properties.isBatchMode() && customizer instanceof ListenerContainerWithDlqAndRetryCustomizer;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected MessageHandler getErrorMessageHandler(final ConsumerDestination destination,
			final String group,
			final ExtendedConsumerProperties<KafkaConsumerProperties> properties) {

		KafkaConsumerProperties kafkaConsumerProperties = properties.getExtension();
		if (kafkaConsumerProperties.isEnableDlq() && !isBatchAndListenerContainerWithDlqAndRetryCustomizer(properties)) {
			KafkaProducerProperties dlqProducerProperties = kafkaConsumerProperties
					.getDlqProducerProperties();
			KafkaAwareTransactionManager<byte[], byte[]> transMan = transactionManager(
					properties.getExtension().getTransactionManager());
			final ExtendedProducerProperties<KafkaProducerProperties> producerProperties =
					new ExtendedProducerProperties<>(dlqProducerProperties);
			producerProperties.populateBindingName(properties.getBindingName());
			ProducerFactory<?, ?> producerFactory = transMan != null
					? transMan.getProducerFactory()
					: getProducerFactory(null, producerProperties,
							destination.getName() + ".dlq.producer", destination.getName());
			final KafkaTemplate<?, ?> kafkaTemplate = new KafkaTemplate<>(producerFactory);
			kafkaTemplate.setObservationEnabled(this.configurationProperties.isEnableObservation());
			kafkaTemplate.setKafkaAdmin(this.kafkaAdmin);

			Object timeout = producerFactory.getConfigurationProperties().get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
			Long sendTimeout = null;
			if (timeout instanceof Number timeoutAsNumber) {
				sendTimeout = timeoutAsNumber.longValue() + 2000L;
			}
			else if (timeout instanceof String timeoutAsString) {
				sendTimeout = Long.parseLong(timeoutAsString) + 2000L;
			}
			if (timeout == null) {
				sendTimeout = ((Integer) ProducerConfig.configDef()
						.defaultValues()
						.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).longValue() + 2000L;
			}
			@SuppressWarnings("rawtypes")
			DlqSender<?, ?> dlqSender = new DlqSender(kafkaTemplate, sendTimeout);

			return (message) -> {
				List<ConsumerRecord<Object, Object>> records;
				if (!properties.isBatchMode()) {
					ConsumerRecord<Object, Object> record = StaticMessageHeaderAccessor.getSourceData(message);
					records = List.of(Objects.requireNonNull(record));
				}
				else {
					records = StaticMessageHeaderAccessor.getSourceData(message);
				}
				if (!CollectionUtils.isEmpty(records)) {
					records.forEach(record ->
						handleRecordForDlq(record, destination, group, properties, kafkaConsumerProperties,
							dlqProducerProperties, transMan, dlqSender, message));
				}
			};
		}
		return null;
	}

	private void handleRecordForDlq(ConsumerRecord<Object, Object> record, ConsumerDestination destination, String group,
									ExtendedConsumerProperties<KafkaConsumerProperties> properties, KafkaConsumerProperties kafkaConsumerProperties,
									KafkaProducerProperties dlqProducerProperties, KafkaAwareTransactionManager<byte[], byte[]> transMan,
									DlqSender<?, ?> dlqSender, Message<?> message) {
		if (properties.isUseNativeDecoding()) {
			if (record != null) {
				// Give the binder configuration the least preference.
				Map<String, String> configuration = this.configurationProperties.getConfiguration();
				// Then give any producer specific properties specified on the binder.
				configuration.putAll(this.configurationProperties.getProducerProperties());
				Map<String, String> configs = transMan == null
						? dlqProducerProperties.getConfiguration()
						: this.configurationProperties.getTransaction()
						.getProducer().getConfiguration();
				Assert.state(!configs.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
						ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " cannot be overridden at the binding level; "
								+ "use multiple binders instead");
				// Finally merge with dlq producer properties or the transaction producer properties.
				configuration.putAll(configs);
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
		Throwable throwable = null;
		if (message.getPayload() instanceof Throwable throwablePayload) {

			throwable = throwablePayload;
			String exceptionMessage = buildMessage(throwable, throwable.getCause());
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
				if (exceptionMessage != null) {
					kafkaHeaders.add(new RecordHeader(X_EXCEPTION_MESSAGE,
							exceptionMessage.getBytes(StandardCharsets.UTF_8)));
				}
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
					messageValues.put(X_EXCEPTION_MESSAGE, exceptionMessage);
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

		MessageHeaders headers;
		if (message instanceof ErrorMessage errorMessage) {
			final Message<?> originalMessage = errorMessage.getOriginalMessage();
			if (originalMessage != null) {
				headers = originalMessage.getHeaders();
			}
			else {
				headers = message.getHeaders();
			}
		}
		else {
			headers = message.getHeaders();
		}
		String dlqName = this.dlqDestinationResolver != null ?
				this.dlqDestinationResolver.apply(recordToSend.get(), new Exception(throwable)) : StringUtils.hasText(kafkaConsumerProperties.getDlqName())
				? kafkaConsumerProperties.getDlqName()
				: "error." + record.topic() + "." + group;
		if (this.transactionTemplate != null) {
			Throwable throwable2 = throwable;
			this.transactionTemplate.executeWithoutResult(status -> {
				dlqSender.sendToDlq(recordToSend.get(), kafkaHeaders, dlqName, group, throwable2,
						determinDlqPartitionFunction(properties.getExtension().getDlqPartitions()),
						headers, this.ackModeInfo.get(destination));
			});
		}
		else {
			dlqSender.sendToDlq(recordToSend.get(), kafkaHeaders, dlqName, group, throwable,
					determinDlqPartitionFunction(properties.getExtension().getDlqPartitions()), headers, this.ackModeInfo.get(destination));
		}
	}

	@Nullable
	private String buildMessage(Throwable exception, Throwable cause) {
		String message = exception.getMessage();
		if (!exception.equals(cause)) {
			if (message != null) {
				message = message + "; ";
			}
			String causeMsg = cause.getMessage();
			if (causeMsg != null) {
				if (message != null) {
					message = message + causeMsg;
				}
				else {
					message = causeMsg;
				}
			}
		}
		return message;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private KafkaAwareTransactionManager<byte[], byte[]> transactionManager(@Nullable String beanName) {
		if (StringUtils.hasText(beanName)) {
			return getApplicationContext().getBean(beanName, KafkaAwareTransactionManager.class);
		}
		return this.transactionManager;
	}

	private DlqPartitionFunction determinDlqPartitionFunction(Integer dlqPartitions) {
		if (this.dlqPartitionFunction != null) {
			return this.dlqPartitionFunction;
		}
		else {
			return DlqPartitionFunction.determineFallbackFunction(dlqPartitions, this.logger);
		}
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
				if (message.getPayload() instanceof MessagingException messagingException) {
					AcknowledgmentCallback ack = StaticMessageHeaderAccessor
						.getAcknowledgmentCallback(messagingException.getFailedMessage());
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
			String consumerGroup, ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties,
			String beanName, String destination) {

		Map<String, Object> props = BindingUtils.createConsumerConfigs(anonymous, consumerGroup, consumerProperties,
				this.configurationProperties);

		this.consumerConfigCustomizers
			.forEach(customizer -> customizer.configure(props, consumerProperties.getBindingName(), destination));
		DefaultKafkaConsumerFactory<Object, Object> factory = new DefaultKafkaConsumerFactory<>(props);
		factory.setBeanName(beanName);
		this.clientFactoryCustomizers.forEach(customizer -> customizer.configure(factory));
		return factory;
	}

	private boolean isAutoCommitOnError(
			ExtendedConsumerProperties<KafkaConsumerProperties> properties) {
		return properties.getExtension().getAutoCommitOnError() != null
				? properties.getExtension().getAutoCommitOnError()
				: false;
	}

	private TopicPartitionOffset[] getTopicPartitionOffsets(
			Collection<PartitionInfo> listenedPartitions,
			ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties,
			ConsumerFactory<?, ?> consumerFactory) {

		final TopicPartitionOffset[] TopicPartitionOffsets =
				new TopicPartitionOffset[listenedPartitions.size()];
		int i = 0;
		SeekPosition seekPosition = null;
		Object resetTo = checkReset(extendedConsumerProperties.getExtension().isResetOffsets(),
				consumerFactory.getConfigurationProperties().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
		if (resetTo != null) {
			seekPosition = "earliest".equals(resetTo) ? SeekPosition.BEGINNING : SeekPosition.END;
		}
		for (PartitionInfo partition : listenedPartitions) {

			TopicPartitionOffsets[i++] = new TopicPartitionOffset(
					partition.topic(), partition.partition(), seekPosition);
		}
		return TopicPartitionOffsets;
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

	public void addConsumerConfigCustomizer(ConsumerConfigCustomizer consumerConfigCustomizer) {
		if (consumerConfigCustomizer != null) {
			this.consumerConfigCustomizers.add(consumerConfigCustomizer);
		}
	}

	public void addProducerConfigCustomizer(ProducerConfigCustomizer producerConfigCustomizer) {
		if (producerConfigCustomizer != null) {
			this.producerConfigCustomizers.add(producerConfigCustomizer);
		}
	}

	List<AbstractMessageListenerContainer<?, ?>> getKafkaMessageListenerContainers() {
		return Collections.unmodifiableList(kafkaMessageListenerContainers);
	}

	final class ProducerConfigurationMessageHandler
			extends KafkaProducerMessageHandler<byte[], byte[]> {

		private boolean running = true;

		private final ProducerFactory<byte[], byte[]> producerFactory;

		PartitionHandler kafkaPartitionHandler = null;

		private String topic;

		ProducerConfigurationMessageHandler(KafkaTemplate<byte[], byte[]> kafkaTemplate,
				String topic,
				ExtendedProducerProperties<KafkaProducerProperties> producerProperties,
				ProducerFactory<byte[], byte[]> producerFactory, ConfigurableListableBeanFactory beanFactory) {

			super(kafkaTemplate);
			this.topic = topic;

			if (producerProperties.getExtension().isUseTopicHeader()) {
				setTopicExpression(PARSER.parseExpression("headers['" + KafkaHeaders.TOPIC + "'] ?: '" + topic + "'"));
			}
			else {
				setTopicExpression(new LiteralExpression(topic));
			}
			Expression messageKeyExpression = producerProperties.getExtension().getMessageKeyExpression();
			if (expressionInterceptorNeeded(producerProperties)) {
				messageKeyExpression = PARSER.parseExpression("headers['"
						+ KafkaExpressionEvaluatingInterceptor.MESSAGE_KEY_HEADER
						+ "']");
			}
			setMessageKeyExpression(messageKeyExpression);
			setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
			if (producerProperties.isPartitioned()) {
				setPartitionIdExpression(PARSER.parseExpression(
						"headers['" + BinderHeaders.PARTITION_HEADER + "']"));
			}
			if (producerProperties.getExtension().isSync()) {
				setSync(true);
			}
			if (producerProperties.getExtension().getSendTimeoutExpression() != null) {
				setSendTimeoutExpression(producerProperties.getExtension().getSendTimeoutExpression());
			}
			this.producerFactory = producerFactory;

			/*
			 	Activate own instance of a PartitionHandler if necessary/possible to  override any other existing
			 	partition calculation (see other usages of PartitionHandler) by	using current partition count
			 	(which may have changed at runtime) each time a message is handled.
			 	PartitionKeyExpression 'payload' is not supported here, because of
			 	OutboundContentTypeConvertingInterceptor would have been called before and the payload will be encoded and
			 	not readable for PartitionHandler during handleMessage method.
			 */
			if (producerProperties.isDynamicPartitionUpdatesEnabled() &&
				producerProperties.getPartitionKeyExpression() != null &&
				!(producerProperties.getPartitionKeyExpression().getExpressionString()
					.toLowerCase(Locale.ROOT).contains("payload"))) {
				kafkaPartitionHandler =
					new PartitionHandler(ExpressionUtils.createStandardEvaluationContext(beanFactory),
						producerProperties, beanFactory);
			}
		}

		@Override
		public void start() {
			try {
				super.onInit();
			}
			catch (Exception ex) {
				this.logger.error(ex, "Initialization errors: ");
				throw new RuntimeException(ex);
			}
		}

		@Override
		public void stop() {
			if (this.producerFactory instanceof DisposableBean disposableProducerFactory) {
				try {
					disposableProducerFactory.destroy();
				}
				catch (Exception ex) {
					this.logger.error(ex, "Error destroying the producer factory bean: ");
					throw new RuntimeException(ex);
				}
			}
			this.running = false;
		}

		@Override
		public boolean isRunning() {
			return this.running;
		}

		@Override
		public void handleMessage(Message<?> message) {

			// if we use our own partition handler to update partition count we recalculate partition
			if (kafkaPartitionHandler != null) {
				kafkaPartitionHandler.setPartitionCount(getKafkaTemplate().partitionsFor(this.topic).size());
				int partitionId = kafkaPartitionHandler.determinePartition(message);

				Message<?> newMessage = MessageBuilder
					.fromMessage(message)
					.setHeader(BinderHeaders.PARTITION_HEADER, partitionId).build();

				super.handleMessage(newMessage);
			}
			else {
				super.handleMessage(message);
			}
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

		private final long sendTimeout;

		DlqSender(KafkaTemplate<K, V> kafkaTemplate, long timeout) {
			this.kafkaTemplate = kafkaTemplate;
			this.sendTimeout = timeout;
		}

		@SuppressWarnings("unchecked")
		void sendToDlq(ConsumerRecord<?, ?> consumerRecord, Headers headers,
					String dlqName, String group, Throwable throwable, DlqPartitionFunction partitionFunction,
					MessageHeaders messageHeaders, ContainerProperties.AckMode ackMode) {
			K key = (K) consumerRecord.key();
			V value = (V) consumerRecord.value();
			ProducerRecord<K, V> producerRecord = new ProducerRecord<>(dlqName,
					partitionFunction.apply(group, consumerRecord, throwable),
					key, value, headers);

			StringBuilder sb = new StringBuilder().append(" a message with key='")
					.append(keyOrValue(key))
					.append("'").append(" and payload='")
					.append(keyOrValue(value))
					.append("'").append(" received from ")
					.append(consumerRecord.partition());
			CompletableFuture<SendResult<K, V>> sentDlq = null;
			try {
				sentDlq = this.kafkaTemplate.send(producerRecord);
				sentDlq.whenComplete((result, ex) -> {
					if (ex != null) {
						KafkaMessageChannelBinder.this.logger.error("Error sending to DLQ " + sb, ex);
					}
					else {
						if (KafkaMessageChannelBinder.this.logger.isDebugEnabled()) {
							KafkaMessageChannelBinder.this.logger.debug("Sent to DLQ " + sb + ": " + result.getRecordMetadata());
						}
					}
				});
				try {
					sentDlq.get(this.sendTimeout, TimeUnit.MILLISECONDS);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw ex;
				}
			}
			catch (Exception ex) {
				KafkaMessageChannelBinder.this.logger
						.error("Error sending to DLQ " + sb.toString(), ex);
			}
			finally {
				if (ackMode == ContainerProperties.AckMode.MANUAL
						|| ackMode == ContainerProperties.AckMode.MANUAL_IMMEDIATE) {
					messageHeaders.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class).acknowledge();
				}
			}
		}

		private String keyOrValue(Object keyOrValue) {
			if (keyOrValue instanceof byte[] keyOrValueBytes) {
				return "byte[" + keyOrValueBytes.length + "]";
			}
			else {
				return toDisplayString(ObjectUtils.nullSafeToString(keyOrValue), 50);
			}
		}
	}
}
