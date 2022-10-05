/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.provisioning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DeclarableCustomizer;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.DeclarationExceptionEvent;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties.QuorumConfig;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties.ContainerType;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties.AlternateExchange;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * AMQP implementation for {@link ProvisioningProvider}.
 *
 * @author Soby Chacko
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Michael Michailidis
 */
// @checkstyle:off
public class RabbitExchangeQueueProvisioner
		implements ApplicationListener<DeclarationExceptionEvent>,
		ProvisioningProvider<ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> {

	// @checkstyle:on

	/**
	 * The delimiter between a group and index when constructing a binder
	 * consumer/producer.
	 */
	private static final String GROUP_INDEX_DELIMITER = ".";

	protected final Log logger = LogFactory.getLog(getClass());

	private final RabbitAdmin rabbitAdmin;

	private boolean notOurAdminException;

	private final GenericApplicationContext autoDeclareContext = new GenericApplicationContext();

	private final List<DeclarableCustomizer> customizers;

	private final AtomicInteger producerExchangeBeanNameQualifier = new AtomicInteger();

	public RabbitExchangeQueueProvisioner(ConnectionFactory connectionFactory) {
		this(connectionFactory, Collections.emptyList());
	}

	public RabbitExchangeQueueProvisioner(ConnectionFactory connectionFactory,
			List<DeclarableCustomizer> customizers) {

		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
		this.autoDeclareContext.refresh();
		this.rabbitAdmin.setApplicationContext(this.autoDeclareContext);
		this.rabbitAdmin.afterPropertiesSet();
		this.customizers = customizers;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties) {
		final String exchangeName = applyPrefix(
				producerProperties.getExtension().getPrefix(), name);
		String beanNameQualifier = "prod" + this.producerExchangeBeanNameQualifier.incrementAndGet();
		Exchange exchange = buildExchange(producerProperties.getExtension(),
				exchangeName, producerProperties.getExtension().getAlternateExchange(), beanNameQualifier);
		if (producerProperties.getExtension().isDeclareExchange()) {
			declareExchange(exchangeName, beanNameQualifier, exchange);
		}
		Binding binding = null;
		for (String requiredGroupName : producerProperties.getRequiredGroups()) {
			String baseQueueName = producerProperties.getExtension()
					.isQueueNameGroupOnly() ? requiredGroupName
							: (exchangeName + "." + requiredGroupName);
			if (!producerProperties.isPartitioned()) {
				autoBindDLQ(baseQueueName, baseQueueName, requiredGroupName,
						producerProperties.getExtension());
				if (producerProperties.getExtension().isBindQueue()) {
					Queue queue = new Queue(baseQueueName, true, false, false, queueArgs(
							baseQueueName, producerProperties.getExtension(), false));
					declareQueue(baseQueueName, queue);
					String[] routingKeys = bindingRoutingKeys(producerProperties.getExtension());
					if (ObjectUtils.isEmpty(routingKeys)) {
						binding = notPartitionedBinding(exchange, queue, null, producerProperties.getExtension());
					}
					else {
						for (String routingKey : routingKeys) {
							binding = notPartitionedBinding(exchange, queue, routingKey,
									producerProperties.getExtension());
						}
					}
				}
			}
			else {
				// if the stream is partitioned, create one queue for each target
				// partition for the default group
				for (int i = 0; i < producerProperties.getPartitionCount(); i++) {
					String partitionSuffix = "-" + i;
					String partitionQueueName = baseQueueName + partitionSuffix;
					autoBindDLQ(baseQueueName, baseQueueName + partitionSuffix, requiredGroupName,
							producerProperties.getExtension());
					if (producerProperties.getExtension().isBindQueue()) {
						Queue queue = new Queue(partitionQueueName, true, false, false,
								queueArgs(partitionQueueName,
										producerProperties.getExtension(), false));
						declareQueue(queue.getName(), queue);
						String prefix = producerProperties.getExtension().getPrefix();
						String destination = StringUtils.isEmpty(prefix) ? exchangeName
								: exchangeName.substring(prefix.length());
						String[] routingKeys = bindingRoutingKeys(producerProperties.getExtension());
						if (ObjectUtils.isEmpty(routingKeys)) {
							binding = partitionedBinding(destination, exchange, queue, null,
								producerProperties.getExtension(), i);
						}
						else {
							for (String routingKey : routingKeys) {
								binding = partitionedBinding(destination, exchange, queue, routingKey,
										producerProperties.getExtension(), i);
							}
						}
					}
				}
			}
		}
		return new RabbitProducerDestination(exchange, binding, beanNameQualifier);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		ConsumerDestination consumerDestination;
		if (!properties.isMultiplex()) {
			consumerDestination = doProvisionConsumerDestination(name, group, properties);
		}
		else {
			String[] provisionedDestinations = Stream
					.of(StringUtils.tokenizeToStringArray(name, ",", true, true))
					.flatMap(destination -> {
						if (properties.isPartitioned() && !ObjectUtils.isEmpty(properties.getInstanceIndexList())) {
							List<String> consumerDestinationNames = new ArrayList<>();

							for (Integer index : properties.getInstanceIndexList()) {
								ExtendedConsumerProperties<RabbitConsumerProperties> temporaryProperties =
										new ExtendedConsumerProperties<>(properties.getExtension());
								BeanUtils.copyProperties(properties, temporaryProperties);
								temporaryProperties.setInstanceIndex(index);
								consumerDestinationNames.add(doProvisionConsumerDestination(destination, group,
										temporaryProperties).getName());
							}

							return consumerDestinationNames.stream();
						}
						else {
							return Stream.of(doProvisionConsumerDestination(destination, group,
									properties).getName());
						}
					})
					.toArray(String[]::new);
			consumerDestination = new RabbitConsumerDestination(
					StringUtils.arrayToCommaDelimitedString(provisionedDestinations),
					null, group, name);
		}
		return consumerDestination;
	}

	private ConsumerDestination doProvisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {

		boolean anonymous = !StringUtils.hasText(group);
		String anonymousGroup = null;
		if (anonymous) {
			anonymousGroup = new Base64UrlNamingStrategy(
					properties.getExtension().getAnonymousGroupPrefix() == null
						? ""
						: properties.getExtension().getAnonymousGroupPrefix()).generateName();
		}
		String baseQueueName;
		if (properties.getExtension().isQueueNameGroupOnly()) {
			baseQueueName = anonymous ? anonymousGroup : group;
		}
		else {
			baseQueueName = groupedName(name, anonymous ? anonymousGroup : group);
		}
		if (this.logger.isInfoEnabled()) {
			this.logger.info("declaring queue for inbound: " + baseQueueName
					+ ", bound to: " + name);
		}
		String prefix = properties.getExtension().getPrefix();
		final String exchangeName = applyPrefix(prefix, name);
		Exchange exchange = buildExchange(properties.getExtension(), exchangeName, null, null);
		if (properties.getExtension().isDeclareExchange()) {
			declareExchange(exchangeName, anonymous ? anonymousGroup : group, exchange);
		}
		String queueName = applyPrefix(prefix, baseQueueName);
		boolean partitioned = !anonymous && properties.isPartitioned();
		boolean durable = !anonymous && properties.getExtension().isDurableSubscription();
		Queue queue;
		if (anonymous) {
			String anonQueueName = queueName;
			queue = new AnonymousQueue((org.springframework.amqp.core.NamingStrategy) () -> anonQueueName,
					queueArgs(queueName, properties.getExtension(), false));
		}
		else {
			if (partitioned) {
				String partitionSuffix = "-" + properties.getInstanceIndex();
				queueName += partitionSuffix;
			}
			if (durable) {
				queue = new Queue(queueName, true, false, false,
						queueArgs(queueName, properties.getExtension(), false));
			}
			else {
				queue = new Queue(queueName, false, false, true,
						queueArgs(queueName, properties.getExtension(), false));
			}
		}
		Binding binding = null;
		if (properties.getExtension().isBindQueue()) {
			if (properties.getExtension().getContainerType().equals(ContainerType.STREAM)) {
				queue.getArguments().put("x-queue-type", "stream");
			}
			declareQueue(queueName, queue);
			String[] routingKeys = bindingRoutingKeys(properties.getExtension());
			if (ObjectUtils.isEmpty(routingKeys)) {
				binding = declareConsumerBindings(name, null, properties, exchange, partitioned, queue);
			}
			else {
				for (String routingKey : routingKeys) {
					binding = declareConsumerBindings(name, routingKey, properties, exchange, partitioned, queue);
				}
			}
		}
		if (durable) {
			autoBindDLQ(applyPrefix(properties.getExtension().getPrefix(), baseQueueName),
					queueName, group, properties.getExtension());
		}
		return new RabbitConsumerDestination(queue.getName(), binding, anonymous ? baseQueueName : group, name);
	}

	/**
	 * Construct a name comprised of the name and group.
	 * @param name the name.
	 * @param group the group.
	 * @return the constructed name.
	 */
	protected final String groupedName(String name, String group) {
		return name + GROUP_INDEX_DELIMITER
				+ (StringUtils.hasText(group) ? group : "default");
	}

	private Binding declareConsumerBindings(String name, String routingKey,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties,
			Exchange exchange, boolean partitioned, Queue queue) {

		if (partitioned) {
			return partitionedBinding(name, exchange, queue, routingKey, properties.getExtension(),
					properties.getInstanceIndex());
		}
		else {
			return notPartitionedBinding(exchange, queue, routingKey, properties.getExtension());
		}
	}

	private Binding partitionedBinding(String destination, Exchange exchange, Queue queue, String rk,
			RabbitCommonProperties extendedProperties, int index) {

		String bindingKey = rk;
		if (bindingKey == null) {
			bindingKey = destination;
		}
		bindingKey += "-" + index;
		Map<String, Object> arguments = new HashMap<>();
		arguments.putAll(extendedProperties.getQueueBindingArguments());
		if (exchange instanceof TopicExchange) {
			Binding binding = BindingBuilder.bind(queue).to((TopicExchange) exchange)
					.with(bindingKey);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else if (exchange instanceof DirectExchange) {
			Binding binding = BindingBuilder.bind(queue).to((DirectExchange) exchange)
					.with(bindingKey);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else if (exchange instanceof FanoutExchange) {
			throw new ProvisioningException(
					"A fanout exchange is not appropriate for partitioned apps");
		}
		else if (exchange instanceof HeadersExchange) {
			Binding binding = new Binding(queue.getName(), DestinationType.QUEUE, exchange.getName(), "", arguments);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else {
			throw new ProvisioningException(
					"Cannot bind to a " + exchange.getType() + " exchange");
		}
	}

	private Binding notPartitionedBinding(Exchange exchange, Queue queue, String rk,
			RabbitCommonProperties extendedProperties) {

		String routingKey = rk;
		if (routingKey == null) {
			routingKey = "#";
		}
		Map<String, Object> arguments = new HashMap<>(extendedProperties.getQueueBindingArguments());
		return createBinding(exchange, queue, routingKey, arguments, queue.getName());
	}

	private Binding createBinding(Exchange exchange, Queue queue, String routingKey,
			@Nullable Map<String, Object> arguments, String beanName) {

		if (exchange instanceof TopicExchange) {
			Binding binding = BindingBuilder.bind(queue).to((TopicExchange) exchange)
					.with(routingKey);
			declareBinding(beanName, binding);
			return binding;
		}
		else if (exchange instanceof DirectExchange) {
			Binding binding = BindingBuilder.bind(queue).to((DirectExchange) exchange)
					.with(routingKey);
			declareBinding(beanName, binding);
			return binding;
		}
		else if (exchange instanceof FanoutExchange) {
			Binding binding = BindingBuilder.bind(queue).to((FanoutExchange) exchange);
			declareBinding(beanName, binding);
			return binding;
		}
		else if (exchange instanceof HeadersExchange) {
			Binding binding = new Binding(beanName, DestinationType.QUEUE, exchange.getName(), "", arguments);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else {
			throw new ProvisioningException(
					"Cannot bind to a " + exchange.getType() + " exchange");
		}
	}

	private String[] bindingRoutingKeys(RabbitCommonProperties extendedProperties) {
		/*
		 * When the delimiter is null, we get a String[1] containing the original.
		 */
		return StringUtils.delimitedListToStringArray(extendedProperties.getBindingRoutingKey(),
				extendedProperties.getBindingRoutingKeyDelimiter());
	}

	/**
	 * If so requested, declare the DLX/DLQ and bind it. The DLQ is bound to the DLX with
	 * a routing key of the original queue name because we use default exchange routing by
	 * queue name for the original message.
	 * @param baseQueueName The base name for the queue (including the binder prefix, if
	 * any).
	 * @param routingKey The routing key for the queue.
	 * @param group The consumer group.
	 * @param properties the properties.
	 */
	private void autoBindDLQ(final String baseQueueName, String routingKey, String group,
			RabbitCommonProperties properties) {
		boolean autoBindDlq = properties.isAutoBindDlq();
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("autoBindDLQ=" + autoBindDlq + " for: " + baseQueueName);
		}
		if (autoBindDlq) {
			String dlqName;
			if (properties.getDeadLetterQueueName() == null) {
				dlqName = constructDLQName(baseQueueName);
			}
			else {
				dlqName = properties.getDeadLetterQueueName();
			}
			Queue dlq = new Queue(dlqName, true, false, false,
					queueArgs(dlqName, properties, true));
			declareQueue(dlqName, dlq);
			String dlxName = deadLetterExchangeName(properties);
			if (properties.isDeclareDlx()) {
				declareExchange(dlxName, group,
						new ExchangeBuilder(dlxName,
								properties.getDeadLetterExchangeType()).durable(true)
										.build());
			}
			Map<String, Object> arguments = new HashMap<>(properties.getDlqBindingArguments());
			Binding dlqBinding = new Binding(dlq.getName(), DestinationType.QUEUE,
					dlxName, properties.getDeadLetterRoutingKey() == null ? routingKey
							: properties.getDeadLetterRoutingKey(),
					arguments);
			declareBinding(dlqName, dlqBinding);
			if (properties instanceof RabbitConsumerProperties
					&& ((RabbitConsumerProperties) properties).isRepublishToDlq()) {
				/*
				 * Also bind with the base queue name when republishToDlq is used, which
				 * does not know about partitioning
				 */
				declareBinding(dlqName + ".2", new Binding(dlq.getName(), DestinationType.QUEUE,
						dlxName, baseQueueName, arguments));
			}
		}
	}

	/**
	 * For binder implementations that support dead lettering, construct the name of the
	 * dead letter entity for the underlying pipe name.
	 * @param name the name.
	 * @return constructDLQName
	 */
	public static String constructDLQName(String name) {
		return name + ".dlq";
	}

	private String deadLetterExchangeName(RabbitCommonProperties properties) {
		if (properties.getDeadLetterExchange() == null) {
			return properties.getPrefix() + RabbitCommonProperties.DEAD_LETTER_EXCHANGE;
		}
		else {
			return properties.getDeadLetterExchange();
		}
	}

	private void declareQueue(String beanName, Queue queueArg) {
		Queue queue = queueArg;
		for (DeclarableCustomizer customizer : this.customizers) {
			queue = (Queue) customizer.apply(queue);
		}
		try {
			this.rabbitAdmin.declareQueue(queue);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of queue: " + queue.getName()
						+ " deferred - connection not available");
			}
		}
		catch (RuntimeException e) {
			if (this.notOurAdminException) {
				this.notOurAdminException = false;
				throw e;
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of queue: " + queue.getName() + " deferred", e);
			}
		}
		addToAutoDeclareContext(beanName, queue);
	}

	private Map<String, Object> queueArgs(String queueName,
			RabbitCommonProperties properties, boolean isDlq) {
		Map<String, Object> args = new HashMap<>();
		if (!isDlq) {
			if (properties.isAutoBindDlq()) {
				String dlx;
				if (properties.getDeadLetterExchange() != null) {
					dlx = properties.getDeadLetterExchange();
				}
				else {
					dlx = applyPrefix(properties.getPrefix(), "DLX");
				}
				args.put("x-dead-letter-exchange", dlx);
				String dlRk;
				if (properties.getDeadLetterRoutingKey() != null) {
					dlRk = properties.getDeadLetterRoutingKey();
				}
				else {
					dlRk = queueName;
				}
				args.put("x-dead-letter-routing-key", dlRk);
			}
		}
		else {
			if (properties.getDlqDeadLetterExchange() != null) {
				args.put("x-dead-letter-exchange", properties.getDlqDeadLetterExchange());
			}
			if (properties.getDlqDeadLetterRoutingKey() != null) {
				args.put("x-dead-letter-routing-key",
						properties.getDlqDeadLetterRoutingKey());
			}
		}
		additionalArgs(args, properties, isDlq);
		return args;
	}

	private void additionalArgs(Map<String, Object> args, RabbitCommonProperties properties, boolean isDlq) {
		Integer expires = isDlq ? properties.getDlqExpires() : properties.getExpires();
		Integer maxLength = isDlq ? properties.getDlqMaxLength()
				: properties.getMaxLength();
		Integer maxLengthBytes = isDlq ? properties.getDlqMaxLengthBytes()
				: properties.getMaxLengthBytes();
		Integer maxPriority = isDlq ? properties.getDlqMaxPriority()
				: properties.getMaxPriority();
		Integer ttl = isDlq ? properties.getDlqTtl() : properties.getTtl();
		boolean lazy = isDlq ? properties.isDlqLazy() : properties.isLazy();
		String overflow = isDlq ? properties.getDlqOverflowBehavior()
				: properties.getOverflowBehavior();
		QuorumConfig quorum = isDlq ? properties.getDlqQuorum() : properties.getQuorum();
		boolean singleActive = isDlq ? properties.isDlqSingleActiveConsumer() : properties.isSingleActiveConsumer();
		if (expires != null) {
			args.put("x-expires", expires);
		}
		if (maxLength != null) {
			args.put("x-max-length", maxLength);
		}
		if (maxLengthBytes != null) {
			args.put("x-max-length-bytes", maxLengthBytes);
		}
		if (maxPriority != null) {
			args.put("x-max-priority", maxPriority);
		}
		if (ttl != null) {
			args.put("x-message-ttl", ttl);
		}
		if (lazy) {
			args.put("x-queue-mode", "lazy");
		}
		if (StringUtils.hasText(overflow)) {
			args.put("x-overflow", overflow);
		}
		if (quorum != null && quorum.isEnabled()) {
			args.put("x-queue-type", "quorum");
			if (quorum.getDeliveryLimit() != null) {
				args.put("x-delivery-limit", quorum.getDeliveryLimit());
			}
			if (quorum.getInitialGroupSize() != null) {
				args.put("x-quorum-initial-group-size", quorum.getInitialGroupSize());
			}
		}
		if (singleActive) {
			args.put("x-single-active-consumer", true);
		}
	}

	public static String applyPrefix(String prefix, String name) {
		return prefix + name;
	}

	private Exchange buildExchange(RabbitCommonProperties properties, String exchangeName,
			@Nullable AlternateExchange alternate, @Nullable String beanNameQualifier) {

		try {
			ExchangeBuilder builder = new ExchangeBuilder(exchangeName,
					properties.getExchangeType());
			builder.durable(properties.isExchangeDurable());
			if (properties.isExchangeAutoDelete()) {
				builder.autoDelete();
			}
			if (properties.isDelayedExchange()) {
				builder.delayed();
			}
			if (alternate != null && !alternate.isExists()) {
				builder.alternate(alternate.getName());
				configureAlternate(alternate, beanNameQualifier);
			}
			return builder.build();
		}
		catch (Exception e) {
			throw new ProvisioningException("Failed to create exchange object", e);
		}
	}

	private void configureAlternate(AlternateExchange alternate, String beanNameQualifier) {
		Exchange exchange = customizeAndDeclare(new ExchangeBuilder(alternate.getName(), alternate.getType())
			.durable(true)
			.build());
		addToAutoDeclareContext(alternate.getName() + "." + beanNameQualifier + ".exchange", exchange);
		AlternateExchange.Binding binding = alternate.getBinding();
		if (binding != null) {
			Queue queue = new Queue(binding.getQueue());
			String beanName = alternate.getName() + "." + binding.getQueue() + "." + beanNameQualifier;
			declareQueue(beanName, queue);
			Binding toBind = createBinding(exchange, queue, binding.getRoutingKey(), null, beanName);
		}
	}

	private void declareExchange(final String rootName, String group, final Exchange exchangeArg) {
		Exchange exchange = customizeAndDeclare(exchangeArg);
		addToAutoDeclareContext(rootName + "." + group + ".exchange", exchange);
	}

	private Exchange customizeAndDeclare(Exchange exchange) {
		for (DeclarableCustomizer customizer : this.customizers) {
			exchange = (Exchange) customizer.apply(exchange);
		}
		try {
			this.rabbitAdmin.declareExchange(exchange);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of exchange: " + exchange.getName()
						+ " deferred - connection not available");
			}
		}
		catch (RuntimeException e) {
			if (this.notOurAdminException) {
				this.notOurAdminException = false;
				throw e;
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of exchange: " + exchange.getName() + " deferred",
						e);
			}
		}
		return exchange;
	}

	private void addToAutoDeclareContext(String name, Declarable bean) {
		synchronized (this.autoDeclareContext) {
			if (!this.autoDeclareContext.containsBean(name)) {
				this.autoDeclareContext.getBeanFactory().registerSingleton(name, new Declarables(bean));
			}
			else {
				this.autoDeclareContext.getBean(name, Declarables.class).getDeclarables().add(bean);
			}
		}
	}

	private void declareBinding(String rootName, org.springframework.amqp.core.Binding bindingArg) {
		Binding binding = bindingArg;
		for (DeclarableCustomizer customizer : this.customizers) {
			binding = (Binding) customizer.apply(binding);
		}
		try {
			this.rabbitAdmin.declareBinding(binding);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of binding: " + rootName
						+ ".binding deferred - connection not available");
			}
		}
		catch (RuntimeException e) {
			if (this.notOurAdminException) {
				this.notOurAdminException = false;
				throw e;
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of binding: " + rootName + ".binding deferred", e);
			}
		}
		addToAutoDeclareContext(rootName + ".binding", binding);
	}

	public void cleanAutoDeclareContext(ConsumerDestination destination,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {

		synchronized (this.autoDeclareContext) {
			Stream.of(StringUtils.tokenizeToStringArray(destination.getName(), ",", true,
					true)).forEach(name -> {
						String group = null;
						String bindingName = null;
						if (destination instanceof RabbitConsumerDestination) {
							group = ((RabbitConsumerDestination) destination).getGroup();
							bindingName = ((RabbitConsumerDestination) destination).getBindingName();
						}
						RabbitConsumerProperties properties = consumerProperties.getExtension();
						String toRemove = properties.isQueueNameGroupOnly() ? bindingName + "." + group : name.trim();
						boolean partitioned = consumerProperties.isPartitioned();
						if (partitioned) {
							toRemove = removePartitionPart(toRemove);
						}
						removeSingleton(toRemove + ".exchange");
						removeQueueAndBindingBeans(properties, name.trim(), "", group, partitioned);
					});
		}
	}

	public void cleanAutoDeclareContext(ProducerDestination dest,
			ExtendedProducerProperties<RabbitProducerProperties> properties) {

		synchronized (this.autoDeclareContext) {
			if (dest instanceof RabbitProducerDestination) {
				String qual = ((RabbitProducerDestination) dest).getBeanNameQualifier();
				removeSingleton(dest.getName() + "." + qual + ".exchange");
				String[] requiredGroups = properties.getRequiredGroups();
				if (!ObjectUtils.isEmpty(requiredGroups)) {
					for (String group : requiredGroups) {
						if (properties.isPartitioned()) {
							for (int i = 0; i < properties.getPartitionCount(); i++) {
								removeQueueAndBindingBeans(properties.getExtension(),
										properties.getExtension().isQueueNameGroupOnly() ? "" : dest.getName(),
												group + "-" + i, group, true);
							}
						}
						else {
							removeQueueAndBindingBeans(properties.getExtension(), dest.getName() + "." + group, "",
									group, false);
						}
					}
				}
				AlternateExchange alternate = properties.getExtension().getAlternateExchange();
				if (alternate != null) {
					removeSingleton(alternate.getName() + "." + qual + ".exchange");
					RabbitProducerProperties.AlternateExchange.Binding binding = alternate.getBinding();
					if (binding != null) {
						removeSingleton(alternate.getName() + "." + binding.getQueue() + "." + qual);
						removeSingleton(alternate.getName() + "." + binding.getQueue() + "." + qual + ".binding");
					}
				}
			}
		}
	}

	private void removeQueueAndBindingBeans(RabbitCommonProperties properties, String name, String suffix,
			String group, boolean partitioned) {

		boolean suffixPresent = StringUtils.hasText(suffix);
		String withSuffix = name + (suffixPresent ? ("." + suffix) : "");
		String nameDotOptional = name;
		if (!StringUtils.hasText(name)) {
			withSuffix = suffix;
		}
		else {
			nameDotOptional = name + ".";
		}
		removeSingleton(withSuffix + ".binding");
		removeSingleton(withSuffix);
		String dlq = (suffixPresent ? nameDotOptional + group : withSuffix) + ".dlq"; // only one DLQ when partitioned
		if (StringUtils.hasText(properties.getDeadLetterQueueName())) {
			dlq = properties.getDeadLetterQueueName();
		}
		else if (partitioned) {
			String removedPart = removePartitionPart(dlq);
			if (!removedPart.endsWith(".dlq")) {
				dlq = removedPart + ".dlq";
			}
		}
		removeSingleton(dlq + ".binding");
		removeSingleton(dlq + ".2.binding");
		removeSingleton(dlq);
		removeSingleton(deadLetterExchangeName(properties) + "." + group + ".exchange");
	}

	private String removePartitionPart(String toRemove) {
		int finalHyphen = toRemove.lastIndexOf("-");
		if (finalHyphen > 0) {
			return toRemove.substring(0, finalHyphen);
		}
		return toRemove;
	}

	private void removeSingleton(String name) {
		if (this.autoDeclareContext.containsBean(name)) {
			ConfigurableListableBeanFactory beanFactory = this.autoDeclareContext
					.getBeanFactory();
			if (beanFactory instanceof DefaultListableBeanFactory) {
				((DefaultListableBeanFactory) beanFactory).destroySingleton(name);
			}
		}
	}

	@Override
	public void onApplicationEvent(DeclarationExceptionEvent event) {
		this.notOurAdminException = true; // our admin doesn't have an event publisher
	}

	private static final class RabbitProducerDestination implements ProducerDestination {

		private final Exchange exchange;

		private final Binding binding;

		private final String beanNameQualifier;

		RabbitProducerDestination(Exchange exchange, Binding binding, String beanNameQualifier) {
			Assert.notNull(exchange, "exchange must not be null");
			this.exchange = exchange;
			this.binding = binding;
			this.beanNameQualifier = beanNameQualifier;
		}

		@Override
		public String getName() {
			return this.exchange.getName();
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.exchange.getName();
		}

		@Nullable
		String getBeanNameQualifier() {
			return this.beanNameQualifier;
		}

		@Override
		public String toString() {
			return "RabbitProducerDestination{" + "exchange=" + this.exchange + ", binding="
					+ this.binding + '}';
		}

	}

	private static final class RabbitConsumerDestination implements ConsumerDestination {

		private final String queue;

		private final Binding binding;

		private final String group;

		private final String bindingName;

		RabbitConsumerDestination(String queue, Binding binding, String group, String bindingName) {
			Assert.notNull(queue, "queue must not be null");
			this.queue = queue;
			this.binding = binding;
			this.group = group;
			this.bindingName = bindingName;
		}

		@Override
		public String getName() {
			return this.queue;
		}

		String getGroup() {
			return this.group;
		}

		String getBindingName() {
			return this.bindingName;
		}

		@Override
		public String toString() {
			return "RabbitConsumerDestination{" + "queue=" + this.queue + ", binding="
					+ this.binding + ", group=" + this.group + ", bindingName=" + this.bindingName + '}';
		}

	}

}
