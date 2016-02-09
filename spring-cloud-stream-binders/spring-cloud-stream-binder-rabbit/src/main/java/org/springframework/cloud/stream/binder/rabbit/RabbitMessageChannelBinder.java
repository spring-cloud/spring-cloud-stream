/*
 * Copyright 2013-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.aopalliance.aop.Advice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.LocalizedQueueConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.DefaultBindingPropertiesAccessor;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 * A {@link org.springframework.cloud.stream.binder.Binder} implementation backed by RabbitMQ.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Jennifer Hickey
 * @author Gunnar Hillert
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Marius Bogoevici
 */
public class RabbitMessageChannelBinder extends AbstractBinder<MessageChannel> {

	public static final AnonymousQueue.Base64UrlNamingStrategy ANONYMOUS_GROUP_NAME_GENERATOR
			= new AnonymousQueue.Base64UrlNamingStrategy("anonymous.");

	private static final AcknowledgeMode DEFAULT_ACKNOWLEDGE_MODE = AcknowledgeMode.AUTO;

	private static final MessageDeliveryMode DEFAULT_DEFAULT_DELIVERY_MODE = MessageDeliveryMode.PERSISTENT;

	private static final boolean DEFAULT_DEFAULT_REQUEUE_REJECTED = true;

	private static final int DEFAULT_MAX_CONCURRENCY = 1;

	private static final int DEFAULT_PREFETCH_COUNT = 1;

	static final String DEFAULT_RABBIT_PREFIX = "binder.";

	private static final int DEFAULT_TX_SIZE = 1;

	private static final String[] DEFAULT_REQUEST_HEADER_PATTERNS = new String[] { "STANDARD_REQUEST_HEADERS", "*" };

	private static final String[] DEFAULT_REPLY_HEADER_PATTERNS = new String[] { "STANDARD_REPLY_HEADERS", "*" };

	private static final String DEAD_LETTER_EXCHANGE = "DLX";

	private static final Set<Object> RABBIT_CONSUMER_PROPERTIES = new HashSet<Object>(Arrays.asList(new String[] {

		BinderPropertyKeys.MAX_CONCURRENCY,
		RabbitPropertiesAccessor.ACK_MODE,
		RabbitPropertiesAccessor.PREFETCH,
		RabbitPropertiesAccessor.PREFIX,
		RabbitPropertiesAccessor.REQUEST_HEADER_PATTERNS,
		RabbitPropertiesAccessor.REQUEUE,
		RabbitPropertiesAccessor.TRANSACTED,
		RabbitPropertiesAccessor.TX_SIZE,
		RabbitPropertiesAccessor.AUTO_BIND_DLQ,
		RabbitPropertiesAccessor.REPUBLISH_TO_DLQ
	}));

	/**
	 * Standard + retry + rabbit consumer properties.
	 */
	private static final Set<Object> SUPPORTED_BASIC_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.addAll(CONSUMER_RETRY_PROPERTIES)
			.addAll(RABBIT_CONSUMER_PROPERTIES)
			.build();

	/**
	 * Basic + durable + concurrency + partitioning.
	 */
	private static final Set<Object> SUPPORTED_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(SUPPORTED_BASIC_CONSUMER_PROPERTIES)
			.add(BinderPropertyKeys.DURABLE)
			.add(BinderPropertyKeys.CONCURRENCY)
			.add(BinderPropertyKeys.PARTITION_INDEX)
			.build();

	/**
	 * Rabbit producer properties.
	 */
	private static final Set<Object> SUPPORTED_BASIC_PRODUCER_PROPERTIES = new SetBuilder()
			.addAll(PRODUCER_STANDARD_PROPERTIES)
			.add(RabbitPropertiesAccessor.DELIVERY_MODE)
			.add(RabbitPropertiesAccessor.PREFIX)
			.add(RabbitPropertiesAccessor.REQUEST_HEADER_PATTERNS)
			.add(BinderPropertyKeys.COMPRESS)
			.build();

	/**
	 * Partitioning + rabbit producer properties.
	 */
	private static final Set<Object> SUPPORTED_PRODUCER_PROPERTIES = new SetBuilder()
			.addAll(PRODUCER_PARTITIONING_PROPERTIES)
			.addAll(SUPPORTED_BASIC_PRODUCER_PROPERTIES)
			.addAll(PRODUCER_BATCHING_BASIC_PROPERTIES)
			.addAll(PRODUCER_BATCHING_ADVANCED_PROPERTIES)
			.add(RabbitPropertiesAccessor.AUTO_BIND_DLQ)
			.build();

	private static final MessagePropertiesConverter inboundMessagePropertiesConverter =
			new DefaultMessagePropertiesConverter() {

				@Override
				public MessageProperties toMessageProperties(AMQP.BasicProperties source, Envelope envelope,
						String charset) {
					MessageProperties properties = super.toMessageProperties(source, envelope, charset);
					properties.setDeliveryMode(null);
					return properties;
				}
			};

	private static final ExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final RabbitAdmin rabbitAdmin;

	private final RabbitTemplate rabbitTemplate = new RabbitTemplate();

	private final GenericApplicationContext autoDeclareContext = new GenericApplicationContext();

	private ConnectionFactory connectionFactory;

	private MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private MessagePostProcessor compressingPostProcessor = new GZipPostProcessor();

	// Default RabbitMQ Container properties

	private volatile AcknowledgeMode defaultAcknowledgeMode = DEFAULT_ACKNOWLEDGE_MODE;

	private volatile boolean defaultChannelTransacted;

	private volatile MessageDeliveryMode defaultDefaultDeliveryMode = DEFAULT_DEFAULT_DELIVERY_MODE;

	private volatile boolean defaultDefaultRequeueRejected = DEFAULT_DEFAULT_REQUEUE_REJECTED;

	private volatile int defaultMaxConcurrency = DEFAULT_MAX_CONCURRENCY;

	private volatile int defaultPrefetchCount = DEFAULT_PREFETCH_COUNT;

	private volatile int defaultTxSize = DEFAULT_TX_SIZE;

	private volatile String defaultPrefix = DEFAULT_RABBIT_PREFIX;

	private volatile String[] defaultRequestHeaderPatterns = DEFAULT_REQUEST_HEADER_PATTERNS;

	private volatile String[] defaultReplyHeaderPatterns = DEFAULT_REPLY_HEADER_PATTERNS;

	private volatile boolean defaultAutoBindDLQ = false;

	private volatile boolean defaultRepublishToDLQ = false;

	private volatile String[] addresses;

	private volatile String[] adminAddresses;

	private volatile String[] nodes;

	private String username;

	private String password;

	private String vhost;

	private boolean useSSL;

	private Resource sslPropertiesLocation;

	private volatile boolean clustered;

	public RabbitMessageChannelBinder(ConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		this.connectionFactory = connectionFactory;
		this.rabbitTemplate.setConnectionFactory(connectionFactory);
		this.rabbitTemplate.afterPropertiesSet();
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
		this.autoDeclareContext.refresh();
		this.rabbitAdmin.setApplicationContext(this.autoDeclareContext);
		this.rabbitAdmin.setIgnoreDeclarationExceptions(true);
		this.rabbitAdmin.afterPropertiesSet();
	}

	/**
	 * Set a {@link MessagePostProcessor} to decompress messages. Defaults to a
	 * {@link DelegatingDecompressingPostProcessor} with its default delegates.
	 * @param decompressingPostProcessor the post processor.
	 */
	public void setDecompressingPostProcessor(MessagePostProcessor decompressingPostProcessor) {
		this.decompressingPostProcessor = decompressingPostProcessor;
	}

	/**
	 * Set a {@link org.springframework.amqp.core.MessagePostProcessor} to compress messages. Defaults to a
	 * {@link org.springframework.amqp.support.postprocessor.GZipPostProcessor}.
	 * @param compressingPostProcessor the post processor.
	 */
	public void setCompressingPostProcessor(MessagePostProcessor compressingPostProcessor) {
		this.compressingPostProcessor = compressingPostProcessor;
	}

	public void setDefaultAcknowledgeMode(AcknowledgeMode defaultAcknowledgeMode) {
		Assert.notNull(defaultAcknowledgeMode, "'defaultAcknowledgeMode' cannot be null");
		this.defaultAcknowledgeMode = defaultAcknowledgeMode;
	}

	public void setDefaultChannelTransacted(boolean defaultChannelTransacted) {
		this.defaultChannelTransacted = defaultChannelTransacted;
	}

	public void setDefaultDefaultDeliveryMode(MessageDeliveryMode defaultDefaultDeliveryMode) {
		Assert.notNull(defaultDefaultDeliveryMode, "'defaultDeliveryMode' cannot be null");
		this.defaultDefaultDeliveryMode = defaultDefaultDeliveryMode;
	}

	public void setDefaultDefaultRequeueRejected(boolean defaultDefaultRequeueRejected) {
		this.defaultDefaultRequeueRejected = defaultDefaultRequeueRejected;
	}

	/**
	 * Set the binder's default max consumers; can be overridden by consumer.maxConcurrency. Values less than 'concurrency'
	 * will be coerced to be equal to concurrency.
	 * @param defaultMaxConcurrency The default max concurrency.
	 */
	public void setDefaultMaxConcurrency(int defaultMaxConcurrency) {
		this.defaultMaxConcurrency = defaultMaxConcurrency;
	}

	public void setDefaultPrefetchCount(int defaultPrefetchCount) {
		this.defaultPrefetchCount = defaultPrefetchCount;
	}

	public void setDefaultTxSize(int defaultTxSize) {
		this.defaultTxSize = defaultTxSize;
	}

	public void setDefaultPrefix(String defaultPrefix) {
		Assert.notNull(defaultPrefix, "'defaultPrefix' cannot be null");
		this.defaultPrefix = defaultPrefix.trim();
	}

	public void setDefaultRequestHeaderPatterns(String[] defaultRequestHeaderPatterns) {
		this.defaultRequestHeaderPatterns = Arrays.copyOf(defaultRequestHeaderPatterns,
				defaultRequestHeaderPatterns.length);
	}

	public void setDefaultReplyHeaderPatterns(String[] defaultReplyHeaderPatterns) {
		this.defaultReplyHeaderPatterns = Arrays.copyOf(defaultReplyHeaderPatterns, defaultReplyHeaderPatterns.length);
	}

	public void setDefaultAutoBindDLQ(boolean defaultAutoBindDLQ) {
		this.defaultAutoBindDLQ = defaultAutoBindDLQ;
	}

	public void setDefaultRepublishToDLQ(boolean defaultRepublishToDLQ) {
		this.defaultRepublishToDLQ = defaultRepublishToDLQ;
	}

	public void setAddresses(String[] addresses) {
		this.addresses = Arrays.copyOf(addresses, addresses.length);
	}

	public void setAdminAddresses(String[] adminAddresses) {
		this.adminAddresses = Arrays.copyOf(adminAddresses, adminAddresses.length);
	}

	public void setNodes(String[] nodes) {
		this.nodes = Arrays.copyOf(nodes, nodes.length);
		this.clustered = nodes.length > 1;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setVhost(String vhost) {
		this.vhost = vhost;
	}

	public void setUseSSL(boolean useSSL) {
		this.useSSL = useSSL;
	}

	public void setSslPropertiesLocation(Resource sslPropertiesLocation) {
		this.sslPropertiesLocation = sslPropertiesLocation;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		if (this.clustered) {
			Assert.state(this.addresses.length == this.adminAddresses.length
					&& this.addresses.length == this.nodes.length,
					"'addresses', 'adminAddresses', and 'nodes' properties must have equal length");
			this.connectionFactory = new LocalizedQueueConnectionFactory(this.connectionFactory, this.addresses,
					this.adminAddresses, this.nodes, this.vhost, this.username, this.password, this.useSSL,
					this.sslPropertiesLocation);
		}
	}

	@Override
	public Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel, Properties properties) {
		boolean anonymousConsumer = !StringUtils.hasText(group);
		String baseQueueName = anonymousConsumer ? groupedName(name, ANONYMOUS_GROUP_NAME_GENERATOR.generateName())
				: groupedName(name, group);
		if (this.logger.isInfoEnabled()) {
			this.logger.info("declaring queue for inbound: " + baseQueueName + ", bound to: " + name);
		}
		validateConsumerProperties(baseQueueName, properties, SUPPORTED_CONSUMER_PROPERTIES);
		RabbitPropertiesAccessor accessor = new RabbitPropertiesAccessor(properties);
		String prefix = accessor.getPrefix(this.defaultPrefix);
		String exchangeName = applyPrefix(prefix, name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);

		String queueName = applyPrefix(prefix, baseQueueName);
		boolean partitioned = !anonymousConsumer && accessor.getPartitionIndex() >= 0;
		boolean durable = !anonymousConsumer && accessor.isDurable(this.defaultDurableSubscription);
		Queue queue;

		if (anonymousConsumer) {
			queue = new Queue(queueName, false, true, true);
		}
		else {
			if (partitioned) {
				String partitionSuffix = "-" + accessor.getPartitionIndex();
				queueName += partitionSuffix;
			}
			if (durable) {
				queue = new Queue(queueName, true, false, false, queueArgs(accessor, queueName));
			}
			else {
				queue = new Queue(queueName, false, false, true);
			}
		}

		declareQueue(queueName, queue);

		if (partitioned) {
			String bindingKey = String.format("%s-%d", name, accessor.getPartitionIndex());
			declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with(bindingKey));
		}
		else {
			declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with("#"));
		}
		Binding<MessageChannel> binding = doRegisterConsumer(baseQueueName, group, inputChannel, queue, accessor);
		if (durable) {
			autoBindDLQ(applyPrefix(prefix, baseQueueName), queueName, accessor);
		}
		return binding;

	}

	private Map<String, Object> queueArgs(RabbitPropertiesAccessor accessor, String queueName) {
		Map<String, Object> args = new HashMap<>();
		if (accessor.getAutoBindDLQ(this.defaultAutoBindDLQ)) {
			args.put("x-dead-letter-exchange", applyPrefix(accessor.getPrefix(this.defaultPrefix), "DLX"));
			args.put("x-dead-letter-routing-key", queueName);
		}
		return args;
	}

	private Binding<MessageChannel> doRegisterConsumer(String name, String group, MessageChannel moduleInputChannel, Queue queue,
			RabbitPropertiesAccessor properties) {
		DefaultBinding<MessageChannel> consumerBinding = null;
		// Fix for XD-2503
		// Temporarily overrides the thread context classloader with the one where the SimpleMessageListenerContainer
		// is defined
		// This allows for the proxying that happens while initializing the SimpleMessageListenerContainer to work
		// correctly
		ClassLoader originalClassloader = Thread.currentThread().getContextClassLoader();
		try {
			ClassUtils.overrideThreadContextClassLoader(SimpleMessageListenerContainer.class.getClassLoader());
			SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(
					this.connectionFactory);
			listenerContainer.setAcknowledgeMode(properties.getAcknowledgeMode(this.defaultAcknowledgeMode));
			listenerContainer.setChannelTransacted(properties.getTransacted(this.defaultChannelTransacted));
			listenerContainer.setDefaultRequeueRejected(properties.getRequeueRejected(this
					.defaultDefaultRequeueRejected));

			int concurrency = properties.getConcurrency(this.defaultConcurrency);
			concurrency = concurrency > 0 ? concurrency : 1;
			listenerContainer.setConcurrentConsumers(concurrency);
			int maxConcurrency = properties.getMaxConcurrency(this.defaultMaxConcurrency);
			if (maxConcurrency > concurrency) {
				listenerContainer.setMaxConcurrentConsumers(maxConcurrency);
			}

			listenerContainer.setPrefetchCount(properties.getPrefetchCount(this.defaultPrefetchCount));
			listenerContainer.setTxSize(properties.getTxSize(this.defaultTxSize));
			listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(queue.getName() + "-"));
			listenerContainer.setQueues(queue);
			int maxAttempts = properties.getMaxAttempts(this.defaultMaxAttempts);
			if (maxAttempts > 1 || properties.getRepublishToDLQ(this.defaultRepublishToDLQ)) {
				RetryOperationsInterceptor retryInterceptor = RetryInterceptorBuilder.stateless()
						.maxAttempts(maxAttempts)
						.backOffOptions(properties.getBackOffInitialInterval(this.defaultBackOffInitialInterval),
								properties.getBackOffMultiplier(this.defaultBackOffMultiplier),
								properties.getBackOffMaxInterval(this.defaultBackOffMaxInterval))
						.recoverer(determineRecoverer(name, properties))
						.build();
				listenerContainer.setAdviceChain(new Advice[] { retryInterceptor });
			}
			listenerContainer.setAfterReceivePostProcessors(this.decompressingPostProcessor);
			listenerContainer.setMessagePropertiesConverter(RabbitMessageChannelBinder.inboundMessagePropertiesConverter);
			listenerContainer.afterPropertiesSet();
			AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(listenerContainer);
			adapter.setBeanFactory(this.getBeanFactory());
			DirectChannel bridgeToModuleChannel = new DirectChannel();
			bridgeToModuleChannel.setBeanFactory(this.getBeanFactory());
			bridgeToModuleChannel.setBeanName(name + ".bridge");
			adapter.setOutputChannel(bridgeToModuleChannel);
			adapter.setBeanName("inbound." + name);
			DefaultAmqpHeaderMapper mapper = new DefaultAmqpHeaderMapper();
			mapper.setRequestHeaderNames(properties.getRequestHeaderPattens(this.defaultRequestHeaderPatterns));
			mapper.setReplyHeaderNames(properties.getReplyHeaderPattens(this.defaultReplyHeaderPatterns));
			adapter.setHeaderMapper(mapper);
			adapter.afterPropertiesSet();
			consumerBinding = DefaultBinding.forConsumer(name, group, adapter, moduleInputChannel, properties,this);
			addBinding(consumerBinding);
			ReceivingHandler convertingBridge = new ReceivingHandler();
			convertingBridge.setOutputChannel(moduleInputChannel);
			convertingBridge.setBeanName(name + ".convert.bridge");
			convertingBridge.afterPropertiesSet();
			bridgeToModuleChannel.subscribe(convertingBridge);
			consumerBinding.getEndpoint().start();
		}
		finally {
			Thread.currentThread().setContextClassLoader(originalClassloader);
		}
		return consumerBinding;
	}

	private MessageRecoverer determineRecoverer(String name, RabbitPropertiesAccessor properties) {
		if (properties.getRepublishToDLQ(this.defaultRepublishToDLQ)) {
			RabbitTemplate errorTemplate = new RabbitTemplate(this.connectionFactory);
			String prefix = properties.getPrefix(this.defaultPrefix);
			RepublishMessageRecoverer republishMessageRecoverer = new RepublishMessageRecoverer(errorTemplate,
					deadLetterExchangeName(prefix),
					applyPrefix(prefix, name));
			return republishMessageRecoverer;
		}
		else {
			return new RejectAndDontRequeueRecoverer();
		}
	}

	private AmqpOutboundEndpoint buildOutboundEndpoint(final String name, RabbitPropertiesAccessor properties,
			RabbitTemplate rabbitTemplate) {
		String prefix = properties.getPrefix(this.defaultPrefix);
		String exchangeName = applyPrefix(prefix, name);
		String partitionKeyExtractorClass = properties.getPartitionKeyExtractorClass();
		Expression partitionKeyExpression = properties.getPartitionKeyExpression();
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);
		AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(rabbitTemplate);
		endpoint.setExchangeName(exchange.getName());
		String baseQueueName = exchangeName + ".default";
		if (partitionKeyExpression == null && !StringUtils.hasText(partitionKeyExtractorClass)) {
			Queue queue = new Queue(baseQueueName, true, false, false, queueArgs(properties, baseQueueName));
			declareQueue(baseQueueName, queue);
			autoBindDLQ(baseQueueName, baseQueueName, properties);
			endpoint.setRoutingKey(name);
			org.springframework.amqp.core.Binding binding = BindingBuilder.bind(queue).to(exchange).with(name);
			declareBinding(baseQueueName, binding);
		}
		else {
			endpoint.setExpressionRoutingKey(EXPRESSION_PARSER.parseExpression(buildPartitionRoutingExpression(name)));
			// if the stream is partitioned, create one queue for each target partition for the default group
			for (int i = 0; i < properties.getNextModuleCount(); i++) {
				String partitionSuffix = "-" + i;
				String partitionQueueName = baseQueueName + partitionSuffix;
				Queue queue = new Queue(partitionQueueName, true, false, false,
						queueArgs(properties, partitionQueueName));
				declareQueue(queue.getName(), queue);
				autoBindDLQ(baseQueueName, baseQueueName + partitionSuffix, properties);
				declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with(name + partitionSuffix));
			}
		}
		configureOutboundHandler(endpoint, properties);
		return endpoint;
	}

	private void configureOutboundHandler(AmqpOutboundEndpoint handler, RabbitPropertiesAccessor properties) {
		DefaultAmqpHeaderMapper mapper = new DefaultAmqpHeaderMapper();
		mapper.setRequestHeaderNames(properties.getRequestHeaderPattens(this.defaultRequestHeaderPatterns));
		mapper.setReplyHeaderNames(properties.getReplyHeaderPattens(this.defaultReplyHeaderPatterns));
		handler.setHeaderMapper(mapper);
		handler.setDefaultDeliveryMode(properties.getDeliveryMode(this.defaultDefaultDeliveryMode));
		handler.setBeanFactory(this.getBeanFactory());
		handler.afterPropertiesSet();
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel outputChannel, Properties properties) {
		validateProducerProperties(name, properties, SUPPORTED_PRODUCER_PROPERTIES);
		RabbitPropertiesAccessor accessor = new RabbitPropertiesAccessor(properties);
		String exchangeName = applyPrefix(accessor.getPrefix(this.defaultPrefix), name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);
		AmqpOutboundEndpoint endpoint = this.buildOutboundEndpoint(name, accessor, determineRabbitTemplate(accessor));
		return doRegisterProducer(name, outputChannel, endpoint, accessor);
	}

	private RabbitTemplate determineRabbitTemplate(RabbitPropertiesAccessor properties) {
		RabbitTemplate rabbitTemplate = null;
		if (properties.isBatchingEnabled(this.defaultBatchingEnabled)) {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
					properties.getBatchSize(this.defaultBatchSize),
					properties.geteBatchBufferLimit(this.defaultBatchBufferLimit),
					properties.getBatchTimeout(this.defaultBatchTimeout));
			rabbitTemplate = new BatchingRabbitTemplate(batchingStrategy,
					getApplicationContext().getBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME,
							TaskScheduler.class));
			rabbitTemplate.setConnectionFactory(this.connectionFactory);
		}
		if (properties.isCompress(this.defaultCompress)) {
			if (rabbitTemplate == null) {
				rabbitTemplate = new RabbitTemplate(this.connectionFactory);
			}
			rabbitTemplate.setBeforePublishPostProcessors(this.compressingPostProcessor);
			rabbitTemplate.afterPropertiesSet();
		}
		if (rabbitTemplate == null) {
			rabbitTemplate = this.rabbitTemplate;
		}
		return rabbitTemplate;
	}

	private Binding<MessageChannel> doRegisterProducer(final String name, MessageChannel moduleOutputChannel,
			AmqpOutboundEndpoint delegate, RabbitPropertiesAccessor properties) {
		return this.doRegisterProducer(name, moduleOutputChannel, delegate, null, properties);
	}

	private Binding<MessageChannel> doRegisterProducer(final String name, MessageChannel moduleOutputChannel,
			AmqpOutboundEndpoint delegate, String replyTo, RabbitPropertiesAccessor properties) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		MessageHandler handler = new SendingHandler(delegate, replyTo, properties);
		EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler);
		consumer.setBeanFactory(getBeanFactory());
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		DefaultBinding<MessageChannel> producerBinding = DefaultBinding.forProducer(name, moduleOutputChannel, consumer, properties, this);
		addBinding(producerBinding);
		producerBinding.getEndpoint().start();
		return producerBinding;
	}

	/**
	 * If so requested, declare the DLX/DLQ and bind it. The DLQ is bound to the DLX with a routing key of the original
	 * queue name because we use default exchange routing by queue name for the original message.
	 * @param queueName The base name for the queue (including the binder prefix, if any).
	 * @param routingKey The routing key for the queue.
	 * @param properties The properties accessor.
	 */
	private void autoBindDLQ(final String queueName, String routingKey, RabbitPropertiesAccessor properties) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("autoBindDLQ=" + properties.getAutoBindDLQ(this.defaultAutoBindDLQ)
					+ " for: " + queueName);
		}
		if (properties.getAutoBindDLQ(this.defaultAutoBindDLQ)) {
			String prefix = properties.getPrefix(this.defaultPrefix);
			String dlqName = constructDLQName(queueName);
			Queue dlq = new Queue(dlqName);
			declareQueue(dlqName, dlq);
			final String dlxName = deadLetterExchangeName(prefix);
			final DirectExchange dlx = new DirectExchange(dlxName);
			declareExchange(dlxName, dlx);
			declareBinding(dlqName, BindingBuilder.bind(dlq).to(dlx).with(routingKey));
		}
	}

	public void declareQueue(String beanName, Queue queue) {
		try {
			this.rabbitAdmin.declareQueue(queue);
		}
		catch (AmqpConnectException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Declaration of queue: " + queue.getName() + " deferred - connection not available");
			}
		}
		catch (UncategorizedAmqpException e) {
			if (e.getCause() instanceof NullPointerException) {
				// Temporary fix for https://jira.spring.io/browse/AMQP-565
				// TODO remove once Spring AMQP is upgraded beyond 1.5.4
			}
			else {
				throw e;
			}
		}
		addToAutoDeclareContext(beanName, queue);
	}

	public void declareExchange(final String rootName, final Exchange exchange) {
		try {
			this.rabbitAdmin.declareExchange(exchange);
		}
		catch (AmqpConnectException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Declaration of exchange: " + exchange.getName() + " deferred - connection not available");
			}
		}
		addToAutoDeclareContext(rootName + ".exchange", exchange);
	}

	public void declareBinding(String rootName, org.springframework.amqp.core.Binding binding) {
		try {
			this.rabbitAdmin.declareBinding(binding);
		}
		catch (AmqpConnectException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Declaration of binding: " + rootName + ".binding deferred - connection not available");
			}
		}
		addToAutoDeclareContext(rootName + ".binding", binding);
	}

	private String deadLetterExchangeName(String prefix) {
		return prefix + DEAD_LETTER_EXCHANGE;
	}

	@Override
	protected void afterUnbind(DefaultBinding<MessageChannel> binding) {
		if (Binding.Type.consumer.equals(binding.getType())) {
			cleanAutoDeclareContext(binding);
		}
	}

	private void addToAutoDeclareContext(String name, Object bean) {
		synchronized (this.autoDeclareContext) {
			if (!this.autoDeclareContext.containsBean(name)) {
				this.autoDeclareContext.getBeanFactory().registerSingleton(name, bean);
			}
		}
	}

	private void cleanAutoDeclareContext(DefaultBinding<MessageChannel> binding) {
		Assert.isTrue(binding.getPropertiesAccessor() instanceof RabbitPropertiesAccessor,
				"Binding was not created by this binder");
		String prefix = ((RabbitPropertiesAccessor) binding.getPropertiesAccessor()).getPrefix(this.defaultPrefix);
		String name = binding.getName();
		synchronized(this.autoDeclareContext) {
			removeSingleton(applyPrefix(prefix,name) + ".binding");
			removeSingleton(applyPrefix(prefix,name));
			String dlq = applyPrefix(prefix,name) + ".dlq";
			removeSingleton(dlq + ".binding");
			removeSingleton(dlq);
		}
	}

	private void removeSingleton(String name) {
		if (this.autoDeclareContext.containsBean(name)) {
			ConfigurableListableBeanFactory beanFactory = this.autoDeclareContext.getBeanFactory();
			if (beanFactory instanceof DefaultListableBeanFactory) {
				((DefaultListableBeanFactory) beanFactory).destroySingleton(name);
			}
		}
	}

	@Override
	public void doManualAck(LinkedList<MessageHeaders> messageHeadersList) {
		Iterator<MessageHeaders> iterator = messageHeadersList.iterator();
		Map<Object, Long> channelsToAck = new HashMap<Object, Long>();
		while (iterator.hasNext()) {
			MessageHeaders messageHeaders = iterator.next();
			if (messageHeaders.containsKey(AmqpHeaders.CHANNEL)) {
				Channel channel = (com.rabbitmq.client.Channel) messageHeaders.get(AmqpHeaders.CHANNEL);
				Long deliveryTag = (Long) messageHeaders.get(AmqpHeaders.DELIVERY_TAG);
				channelsToAck.put(channel, deliveryTag);
			}
		}
		for (Map.Entry<Object, Long> entry : channelsToAck.entrySet()) {
			try {
				((Channel) entry.getKey()).basicAck(entry.getValue(), true);
			}
			catch (IOException e) {
				this.logger.error("Exception while manually acknowledging " + e);
			}
		}
	}

	private class SendingHandler extends AbstractMessageHandler implements Lifecycle {

		private final MessageHandler delegate;

		private final String replyTo;

		private final PartitioningMetadata partitioningMetadata;

		private SendingHandler(MessageHandler delegate, String replyTo, RabbitPropertiesAccessor properties) {
			this.delegate = delegate;
			this.replyTo = replyTo;
			this.partitioningMetadata = new PartitioningMetadata(properties, properties.getNextModuleCount());
			this.setBeanFactory(RabbitMessageChannelBinder.this.getBeanFactory());
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			MessageValues messageToSend = serializePayloadIfNecessary(message);

			if (this.replyTo != null) {
				messageToSend.put(AmqpHeaders.REPLY_TO, this.replyTo);
			}
			if (this.partitioningMetadata.isPartitionedModule()) {
				messageToSend.put(PARTITION_HEADER, determinePartition(message, this.partitioningMetadata));
			}

			this.delegate.handleMessage(messageToSend.toMessage(getMessageBuilderFactory()));
		}

		@Override
		public void start() {
			if (this.delegate instanceof Lifecycle) {
				((Lifecycle) this.delegate).start();
			}
		}

		@Override
		public void stop() {
			if (this.delegate instanceof Lifecycle) {
				((Lifecycle) this.delegate).stop();
			}
		}

		@Override
		public boolean isRunning() {
			if (this.delegate instanceof Lifecycle) {
				return ((Lifecycle) this.delegate).isRunning();
			}
			else {
				return true;
			}
		}

	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		public ReceivingHandler() {
			super();
			this.setBeanFactory(RabbitMessageChannelBinder.this.getBeanFactory());
		}

		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			return deserializePayloadIfNecessary(requestMessage).toMessage(getMessageBuilderFactory());
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			/*
			 * we've already copied the headers so no need for the ARPMH to do it, and we don't want the content-type
			 * restored if absent.
			 */
			return false;
		}

	}

	/**
	 * Property accessor for the RabbitBinder. Refer to the Spring-AMQP documentation for information on the
	 * specific properties.
	 */
	private static class RabbitPropertiesAccessor extends DefaultBindingPropertiesAccessor {

		/**
		 * The acknowledge mode (i.e. NONE, MANUAL, AUTO).
		 */
		private static final String ACK_MODE = "ackMode";

		/**
		 * The delivery mode (i.e. NON_PERSISTENT, PERSISTENT).
		 */
		private static final String DELIVERY_MODE = "deliveryMode";

		/**
		 * The prefetch count (basic qos).
		 */
		private static final String PREFETCH = "prefetch";

		/**
		 * The prefix for queues, exchanges.
		 */
		private static final String PREFIX = "prefix";

		/**
		 * The reply header patterns.
		 */
		private static final String REPLY_HEADER_PATTERNS = "replyHeaderPatterns";

		/**
		 * The request header patterns.
		 */
		private static final String REQUEST_HEADER_PATTERNS = "requestHeaderPatterns";

		/**
		 * Whether delivery failures should be requeued (boolean).
		 */
		private static final String REQUEUE = "requeue";

		/**
		 * Whether to use transacted channels (boolean).
		 */
		private static final String TRANSACTED = "transacted";

		/**
		 * The number of deliveries between acks.
		 */
		private static final String TX_SIZE = "txSize";

		/**
		 * Whether to automatically declare the DLQ and bind it to the binder DLX (boolean).
		 */
		private static final String AUTO_BIND_DLQ = "autoBindDLQ";

		/**
		 * Whether to automatically declare the DLQ and bind it to the binder DLX (boolean).
		 */
		private static final String REPUBLISH_TO_DLQ = "republishToDLQ";

		public RabbitPropertiesAccessor(Properties properties) {
			super(properties);
		}

		public AcknowledgeMode getAcknowledgeMode(AcknowledgeMode defaultValue) {
			String ackknowledgeMode = getProperty(ACK_MODE);
			if (StringUtils.hasText(ackknowledgeMode)) {
				return AcknowledgeMode.valueOf(ackknowledgeMode);
			}
			else {
				return defaultValue;
			}
		}

		public MessageDeliveryMode getDeliveryMode(MessageDeliveryMode defaultValue) {
			String deliveryMode = getProperty(DELIVERY_MODE);
			if (StringUtils.hasText(deliveryMode)) {
				return MessageDeliveryMode.valueOf(deliveryMode);
			}
			else {
				return defaultValue;
			}
		}

		public int getPrefetchCount(int defaultValue) {
			return getProperty(PREFETCH, defaultValue);
		}

		public String getPrefix(String defaultValue) {
			return getProperty(PREFIX, defaultValue);
		}

		public String[] getReplyHeaderPattens(String[] defaultValue) {
			return asStringArray(getProperty(REPLY_HEADER_PATTERNS), defaultValue);
		}

		public String[] getRequestHeaderPattens(String[] defaultValue) {
			return asStringArray(getProperty(REQUEST_HEADER_PATTERNS), defaultValue);
		}

		public boolean getRequeueRejected(boolean defaultValue) {
			return getProperty(REQUEUE, defaultValue);
		}

		public boolean getTransacted(boolean defaultValue) {
			return getProperty(TRANSACTED, defaultValue);
		}

		public int getTxSize(int defaultValue) {
			return getProperty(TX_SIZE, defaultValue);
		}

		public boolean getAutoBindDLQ(boolean defaultValue) {
			return getProperty(AUTO_BIND_DLQ, defaultValue);
		}

		public boolean getRepublishToDLQ(boolean defaultValue) {
			return getProperty(REPUBLISH_TO_DLQ, defaultValue);
		}

	}

}
