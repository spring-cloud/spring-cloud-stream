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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.aopalliance.aop.Advice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
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
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
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
import org.springframework.util.StringUtils;

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
public class RabbitMessageChannelBinder extends AbstractBinder<MessageChannel, ExtendedConsumerProperties<RabbitConsumerProperties>,
		ExtendedProducerProperties<RabbitProducerProperties>> implements ExtendedPropertiesBinder<MessageChannel, RabbitConsumerProperties, RabbitProducerProperties> {

	public static final AnonymousQueue.Base64UrlNamingStrategy ANONYMOUS_GROUP_NAME_GENERATOR
			= new AnonymousQueue.Base64UrlNamingStrategy("anonymous.");

	private static final String DEAD_LETTER_EXCHANGE = "DLX";

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

	private final GenericApplicationContext autoDeclareContext = new GenericApplicationContext();

	private ConnectionFactory connectionFactory;

	private MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private MessagePostProcessor compressingPostProcessor = new GZipPostProcessor();

	private volatile String[] addresses;

	private volatile String[] adminAddresses;

	private volatile String[] nodes;

	private String username;

	private String password;

	private String vhost;

	private boolean useSSL;

	private Resource sslPropertiesLocation;

	private volatile boolean clustered;

	private RabbitExtendedPropertiesRegistry rabbitExtendedPropertiesRegistry = new RabbitExtendedPropertiesRegistry();

	public RabbitMessageChannelBinder(ConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		this.connectionFactory = connectionFactory;
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

	public void setRabbitExtendedPropertiesRegistry(
			RabbitExtendedPropertiesRegistry rabbitExtendedPropertiesRegistry) {
		this.rabbitExtendedPropertiesRegistry = rabbitExtendedPropertiesRegistry;
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
	public void onInit() {
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
	public RabbitConsumerProperties getExtendedConsumerProperties(String channelName) {
		return rabbitExtendedPropertiesRegistry.getExtendedConsumerProperties(channelName);
	}


	@Override
	public RabbitProducerProperties getExtendedProducerProperties(String channelName) {
		return rabbitExtendedPropertiesRegistry.getExtendedProducerProperties(channelName);
	}

	@Override
	public Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		boolean anonymousConsumer = !StringUtils.hasText(group);
		String baseQueueName = anonymousConsumer ? groupedName(name, ANONYMOUS_GROUP_NAME_GENERATOR.generateName())
				: groupedName(name, group);
		if (this.logger.isInfoEnabled()) {
			this.logger.info("declaring queue for inbound: " + baseQueueName + ", bound to: " + name);
		}
		String prefix = properties.getExtension().getPrefix();
		String exchangeName = applyPrefix(prefix, name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);

		String queueName = applyPrefix(prefix, baseQueueName);
		boolean partitioned = !anonymousConsumer && properties.isPartitioned();
		boolean durable = !anonymousConsumer && properties.getExtension().isDurableSubscription();
		Queue queue;

		if (anonymousConsumer) {
			queue = new Queue(queueName, false, true, true);
		}
		else {
			if (partitioned) {
				String partitionSuffix = "-" + properties.getInstanceIndex();
				queueName += partitionSuffix;
			}
			if (durable) {
				queue = new Queue(queueName, true, false, false,
						queueArgs(queueName, properties.getExtension().getPrefix(), properties.getExtension().isAutoBindDlq()));
			}
			else {
				queue = new Queue(queueName, false, false, true);
			}
		}

		declareQueue(queueName, queue);

		if (partitioned) {
			String bindingKey = String.format("%s-%d", name, properties.getInstanceIndex());
			declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with(bindingKey));
		}
		else {
			declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with("#"));
		}
		Binding<MessageChannel> binding = doRegisterConsumer(baseQueueName, group, inputChannel, queue, properties);
		if (durable) {
			autoBindDLQ(applyPrefix(prefix, baseQueueName), queueName, properties.getExtension().getPrefix(), properties.getExtension().isAutoBindDlq());
		}
		return binding;
	}

	private Map<String, Object> queueArgs(String queueName, String prefix, boolean bindDlq) {
		Map<String, Object> args = new HashMap<>();
		if (bindDlq) {
			args.put("x-dead-letter-exchange", applyPrefix(prefix, "DLX"));
			args.put("x-dead-letter-routing-key", queueName);
		}
		return args;
	}

	private Binding<MessageChannel> doRegisterConsumer(final String name, String group, MessageChannel moduleInputChannel, Queue queue,
			final ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		DefaultBinding<MessageChannel> consumerBinding;
		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(
				this.connectionFactory);
		listenerContainer.setAcknowledgeMode(properties.getExtension().getAcknowledgeMode());
		listenerContainer.setChannelTransacted(properties.getExtension().isTransacted());
		listenerContainer.setDefaultRequeueRejected(properties.getExtension().isRequeueRejected());

		int concurrency = properties.getConcurrency();
		concurrency = concurrency > 0 ? concurrency : 1;
		listenerContainer.setConcurrentConsumers(concurrency);
		int maxConcurrency = properties.getExtension().getMaxConcurrency();
		if (maxConcurrency > concurrency) {
			listenerContainer.setMaxConcurrentConsumers(maxConcurrency);
		}

		listenerContainer.setPrefetchCount(properties.getExtension().getPrefetch());
		listenerContainer.setTxSize(properties.getExtension().getTxSize());
		listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(queue.getName() + "-"));
		listenerContainer.setQueues(queue);
		int maxAttempts = properties.getMaxAttempts();
		if (maxAttempts > 1 || properties.getExtension().isRepublishToDlq()) {
			RetryOperationsInterceptor retryInterceptor = RetryInterceptorBuilder.stateless()
					.maxAttempts(maxAttempts)
					.backOffOptions(properties.getBackOffInitialInterval(),
							properties.getBackOffMultiplier(),
							properties.getBackOffMaxInterval())
					.recoverer(determineRecoverer(name, properties.getExtension().getPrefix(), properties.getExtension().isRepublishToDlq()))
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
		mapper.setRequestHeaderNames(properties.getExtension().getRequestHeaderPatterns());
		mapper.setReplyHeaderNames(properties.getExtension().getReplyHeaderPatterns());
		adapter.setHeaderMapper(mapper);
		adapter.afterPropertiesSet();
		consumerBinding = new DefaultBinding<MessageChannel>(name, group, moduleInputChannel, adapter) {
			@Override
			protected void afterUnbind() {
				cleanAutoDeclareContext(properties.getExtension().getPrefix(), name);
			}
		};
		ReceivingHandler convertingBridge = new ReceivingHandler();
		convertingBridge.setOutputChannel(moduleInputChannel);
		convertingBridge.setBeanName(name + ".convert.bridge");
		convertingBridge.afterPropertiesSet();
		bridgeToModuleChannel.subscribe(convertingBridge);
		adapter.start();
		return consumerBinding;
	}

	private MessageRecoverer determineRecoverer(String name, String prefix, boolean republish) {
		if (republish) {
			RabbitTemplate errorTemplate = new RabbitTemplate(this.connectionFactory);
			RepublishMessageRecoverer republishMessageRecoverer = new RepublishMessageRecoverer(errorTemplate,
					deadLetterExchangeName(prefix),
					applyPrefix(prefix, name));
			return republishMessageRecoverer;
		}
		else {
			return new RejectAndDontRequeueRecoverer();
		}
	}

	private AmqpOutboundEndpoint buildOutboundEndpoint(final String name,
			ExtendedProducerProperties<RabbitProducerProperties> properties,
													   RabbitTemplate rabbitTemplate) {
		String prefix = properties.getExtension().getPrefix();
		String exchangeName = applyPrefix(prefix, name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);
		AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(rabbitTemplate);
		endpoint.setExchangeName(exchange.getName());
		if (!properties.isPartitioned()) {
			endpoint.setRoutingKey(name);
		}
		else {
			endpoint.setExpressionRoutingKey(EXPRESSION_PARSER.parseExpression(buildPartitionRoutingExpression(name)));
		}
		for (String requiredGroupName : properties.getRequiredGroups()) {
			String baseQueueName = exchangeName + "." + requiredGroupName;
			if (!properties.isPartitioned()) {
				Queue queue = new Queue(baseQueueName, true, false, false, queueArgs(baseQueueName, prefix, properties.getExtension().isAutoBindDlq()));
				declareQueue(baseQueueName, queue);
				autoBindDLQ(baseQueueName, baseQueueName, properties.getExtension().getPrefix(), properties.getExtension().isAutoBindDlq());
				org.springframework.amqp.core.Binding binding = BindingBuilder.bind(queue).to(exchange).with(name);
				declareBinding(baseQueueName, binding);
			}
			else {
				// if the stream is partitioned, create one queue for each target partition for the default group
				for (int i = 0; i < properties.getPartitionCount(); i++) {
					String partitionSuffix = "-" + i;
					String partitionQueueName = baseQueueName + partitionSuffix;
					Queue queue = new Queue(partitionQueueName, true, false, false,
							queueArgs(partitionQueueName, properties.getExtension().getPrefix(), properties.getExtension().isAutoBindDlq()));
					declareQueue(queue.getName(), queue);
					autoBindDLQ(baseQueueName, baseQueueName + partitionSuffix, properties.getExtension().getPrefix(), properties.getExtension().isAutoBindDlq());
					declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with(name + partitionSuffix));
				}
			}
		}
		configureOutboundHandler(endpoint, properties);
		return endpoint;
	}

	private void configureOutboundHandler(AmqpOutboundEndpoint handler,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties) {
		DefaultAmqpHeaderMapper mapper = new DefaultAmqpHeaderMapper();
		mapper.setRequestHeaderNames(producerProperties.getExtension().getRequestHeaderPatterns());
		mapper.setReplyHeaderNames(producerProperties.getExtension().getReplyHeaderPatterns());
		handler.setHeaderMapper(mapper);
		handler.setDefaultDeliveryMode(producerProperties.getExtension().getDeliveryMode());
		handler.setBeanFactory(this.getBeanFactory());
		handler.afterPropertiesSet();
	}

	@Override
	public Binding<MessageChannel> doBindProducer(String name, MessageChannel outputChannel,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties) {
		String exchangeName = applyPrefix(producerProperties.getExtension().getPrefix(), name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);
		AmqpOutboundEndpoint endpoint = this.buildOutboundEndpoint(name, producerProperties,
				buildRabbitTemplate(producerProperties.getExtension()));
		return doRegisterProducer(name, outputChannel, endpoint, producerProperties);
	}

	private RabbitTemplate buildRabbitTemplate(RabbitProducerProperties properties) {
		RabbitTemplate rabbitTemplate = null;
		if (properties.isBatchingEnabled()) {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
					properties.getBatchSize(),
					properties.getBatchBufferLimit(),
					properties.getBatchTimeout());
			rabbitTemplate = new BatchingRabbitTemplate(batchingStrategy,
					getApplicationContext().getBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME,
							TaskScheduler.class));
		}
		else {
			rabbitTemplate = new RabbitTemplate();
		}
		rabbitTemplate.setConnectionFactory(this.connectionFactory);
		if (properties.isCompress()) {
			rabbitTemplate.setBeforePublishPostProcessors(this.compressingPostProcessor);
		}
		rabbitTemplate.setChannelTransacted(properties.isTransacted());
		rabbitTemplate.afterPropertiesSet();
		return rabbitTemplate;
	}

	private Binding<MessageChannel> doRegisterProducer(final String name, MessageChannel moduleOutputChannel,
			AmqpOutboundEndpoint delegate, ExtendedProducerProperties<RabbitProducerProperties> properties) {
		return this.doRegisterProducer(name, moduleOutputChannel, delegate, null, properties);
	}

	private Binding<MessageChannel> doRegisterProducer(final String name, MessageChannel moduleOutputChannel,
			AmqpOutboundEndpoint delegate, String replyTo,
			ExtendedProducerProperties<RabbitProducerProperties> properties) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		MessageHandler handler = new SendingHandler(delegate, replyTo, properties);
		EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler);
		consumer.setBeanFactory(getBeanFactory());
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		DefaultBinding<MessageChannel> producerBinding = new DefaultBinding<>(name, null, moduleOutputChannel, consumer);
		consumer.start();
		return producerBinding;
	}

	/**
	 * If so requested, declare the DLX/DLQ and bind it. The DLQ is bound to the DLX with a routing key of the original
	 * queue name because we use default exchange routing by queue name for the original message.
	 * @param queueName The base name for the queue (including the binder prefix, if any).
	 * @param routingKey The routing key for the queue.
	 * @param autoBindDlq true if the DLQ should be bound.
	 */
	private void autoBindDLQ(final String queueName, String routingKey, String prefix, boolean autoBindDlq) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("autoBindDLQ=" + autoBindDlq
					+ " for: " + queueName);
		}
		if (autoBindDlq) {
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

	private void addToAutoDeclareContext(String name, Object bean) {
		synchronized (this.autoDeclareContext) {
			if (!this.autoDeclareContext.containsBean(name)) {
				this.autoDeclareContext.getBeanFactory().registerSingleton(name, bean);
			}
		}
	}

	public void cleanAutoDeclareContext(String prefix, String name) {
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

		private final ExtendedProducerProperties<RabbitProducerProperties> producerProperties;

		private final PartitionHandler partitionHandler;

		private SendingHandler(MessageHandler delegate, String replyTo,
				ExtendedProducerProperties<RabbitProducerProperties> properties) {
			this.delegate = delegate;
			this.replyTo = replyTo;
			producerProperties = properties;
			ConfigurableListableBeanFactory beanFactory = RabbitMessageChannelBinder.this.getBeanFactory();
			this.setBeanFactory(beanFactory);
			this.partitionHandler = new PartitionHandler(beanFactory, evaluationContext, partitionSelector,
					properties);
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			MessageValues messageToSend = serializePayloadIfNecessary(message);

			if (this.replyTo != null) {
				messageToSend.put(AmqpHeaders.REPLY_TO, this.replyTo);
			}
			if (producerProperties.isPartitioned()) {
				messageToSend.put(PARTITION_HEADER,
						this.partitionHandler.determinePartition(message));
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

}
