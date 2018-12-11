/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.provisioning;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.DeclarationExceptionEvent;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * AMQP implementation for {@link ProvisioningProvider}
 *
 * @author Soby Chacko
 * @author Gary Russell
 * @author Oleg Zhurakousky
 */
public class RabbitExchangeQueueProvisioner implements ApplicationListener<DeclarationExceptionEvent>,
		ProvisioningProvider<ExtendedConsumerProperties<RabbitConsumerProperties>,
							ExtendedProducerProperties<RabbitProducerProperties>> {

	private static final Base64UrlNamingStrategy ANONYMOUS_GROUP_NAME_GENERATOR
			= new Base64UrlNamingStrategy("anonymous.");

	/**
	 * The delimiter between a group and index when constructing a binder
	 * consumer/producer.
	 */
	private static final String GROUP_INDEX_DELIMITER = ".";

	protected final Log logger = LogFactory.getLog(getClass());

	private final RabbitAdmin rabbitAdmin;

	private boolean notOurAdminException;

	private final GenericApplicationContext autoDeclareContext = new GenericApplicationContext();

	public RabbitExchangeQueueProvisioner(ConnectionFactory connectionFactory) {
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
		this.autoDeclareContext.refresh();
		this.rabbitAdmin.setApplicationContext(this.autoDeclareContext);
		this.rabbitAdmin.afterPropertiesSet();
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
															ExtendedProducerProperties<RabbitProducerProperties> producerProperties) {
		final String exchangeName = applyPrefix(producerProperties.getExtension().getPrefix(), name);
		Exchange exchange = buildExchange(producerProperties.getExtension(), exchangeName);
		if (producerProperties.getExtension().isDeclareExchange()) {
			declareExchange(exchangeName, exchange);
		}
		Binding binding = null;
		for (String requiredGroupName : producerProperties.getRequiredGroups()) {
			String baseQueueName = producerProperties.getExtension().isQueueNameGroupOnly()
					? requiredGroupName : (exchangeName + "." + requiredGroupName);
			if (!producerProperties.isPartitioned()) {
				autoBindDLQ(baseQueueName, baseQueueName, producerProperties.getExtension());
				if (producerProperties.getExtension().isBindQueue()) {
					Queue queue = new Queue(baseQueueName, true, false, false,
							queueArgs(baseQueueName, producerProperties.getExtension(), false));
					declareQueue(baseQueueName, queue);
					binding = notPartitionedBinding(exchange, queue, producerProperties.getExtension());
				}
			}
			else {
				// if the stream is partitioned, create one queue for each target partition for the default group
				for (int i = 0; i < producerProperties.getPartitionCount(); i++) {
					String partitionSuffix = "-" + i;
					String partitionQueueName = baseQueueName + partitionSuffix;
					autoBindDLQ(baseQueueName, baseQueueName + partitionSuffix, producerProperties.getExtension());
					if (producerProperties.getExtension().isBindQueue()) {
						Queue queue = new Queue(partitionQueueName, true, false, false,
								queueArgs(partitionQueueName, producerProperties.getExtension(), false));
						declareQueue(queue.getName(), queue);
						String prefix = producerProperties.getExtension().getPrefix();
						String destination = StringUtils.isEmpty(prefix) ? exchangeName : exchangeName.substring(prefix.length());
						binding = partitionedBinding(destination, exchange, queue, producerProperties.getExtension(), i);
					}
				}
			}
		}
		return new RabbitProducerDestination(exchange, binding);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		ConsumerDestination consumerDestination;
		if (!properties.isMultiplex()) {
			consumerDestination = doProvisionConsumerDestination(name, group, properties);
		}
		else {
			String[] provisionedDestinations = Stream.of(StringUtils.tokenizeToStringArray(name, ",", true, true))
				.map(destination -> doProvisionConsumerDestination(destination, group, properties).getName())
				.toArray(String[]::new);
			consumerDestination = new RabbitConsumerDestination(StringUtils.arrayToCommaDelimitedString(provisionedDestinations), null);
		}
		return consumerDestination;
	}

	private ConsumerDestination doProvisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		boolean anonymous = !StringUtils.hasText(group);
		String  baseQueueName;
		if (properties.getExtension().isQueueNameGroupOnly()) {
				baseQueueName =  anonymous ? ANONYMOUS_GROUP_NAME_GENERATOR.generateName() : group;
		}
		else {
				baseQueueName = groupedName(name, anonymous ? ANONYMOUS_GROUP_NAME_GENERATOR.generateName() : group);
		}
		if (this.logger.isInfoEnabled()) {
			this.logger.info("declaring queue for inbound: " + baseQueueName + ", bound to: " + name);
		}
		String prefix = properties.getExtension().getPrefix();
		final String exchangeName = applyPrefix(prefix, name);
		Exchange exchange = buildExchange(properties.getExtension(), exchangeName);
		if (properties.getExtension().isDeclareExchange()) {
			declareExchange(exchangeName, exchange);
		}
		String queueName = applyPrefix(prefix, baseQueueName);
		boolean partitioned = !anonymous && properties.isPartitioned();
		boolean durable = !anonymous && properties.getExtension().isDurableSubscription();
		Queue queue;
		if (anonymous) {
			queue = new Queue(queueName, false, true, true, queueArgs(queueName, properties.getExtension(), false));
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
			declareQueue(queueName, queue);
			binding = declareConsumerBindings(name, properties, exchange, partitioned, queue);
		}
		if (durable) {
			autoBindDLQ(applyPrefix(properties.getExtension().getPrefix(), baseQueueName), queueName,
					properties.getExtension());
		}
		return new RabbitConsumerDestination(queue.getName(), binding);
	}

	/**
	 * Construct a name comprised of the name and group.
	 *
	 * @param name  the name.
	 * @param group the group.
	 * @return the constructed name.
	 */
	protected final String groupedName(String name, String group) {
		return name + GROUP_INDEX_DELIMITER + (StringUtils.hasText(group) ? group : "default");
	}

	private Binding partitionedBinding(String destination, Exchange exchange, Queue queue,
									RabbitCommonProperties extendedProperties, int index) {
		String bindingKey = extendedProperties.getBindingRoutingKey();
		if (bindingKey == null) {
			bindingKey = destination;
		}
		bindingKey += "-" + index;
		if (exchange instanceof TopicExchange) {
			Binding binding = BindingBuilder.bind(queue)
					.to((TopicExchange) exchange)
					.with(bindingKey);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else if (exchange instanceof DirectExchange) {
			Binding binding = BindingBuilder.bind(queue)
					.to((DirectExchange) exchange)
					.with(bindingKey);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else if (exchange instanceof FanoutExchange) {
			throw new ProvisioningException("A fanout exchange is not appropriate for partitioned apps");
		}
		else {
			throw new ProvisioningException("Cannot bind to a " + exchange.getType() + " exchange");
		}
	}

	private Binding declareConsumerBindings(String name, ExtendedConsumerProperties<RabbitConsumerProperties> properties,
										Exchange exchange, boolean partitioned, Queue queue) {
		if (partitioned) {
			return partitionedBinding(name, exchange, queue, properties.getExtension(), properties.getInstanceIndex());
		}
		else {
			return notPartitionedBinding(exchange, queue, properties.getExtension());
		}
	}

	private Binding notPartitionedBinding(Exchange exchange, Queue queue, RabbitCommonProperties extendedProperties) {
		String routingKey = extendedProperties.getBindingRoutingKey();
		if (routingKey == null) {
			routingKey = "#";
		}
		if (exchange instanceof TopicExchange) {
			Binding binding = BindingBuilder.bind(queue)
					.to((TopicExchange) exchange)
					.with(routingKey);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else if (exchange instanceof DirectExchange) {
			Binding binding = BindingBuilder.bind(queue)
					.to((DirectExchange) exchange)
					.with(routingKey);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else if (exchange instanceof FanoutExchange) {
			Binding binding = BindingBuilder.bind(queue)
					.to((FanoutExchange) exchange);
			declareBinding(queue.getName(), binding);
			return binding;
		}
		else {
			throw new ProvisioningException("Cannot bind to a " + exchange.getType() + " exchange");
		}
	}

	/**
	 * If so requested, declare the DLX/DLQ and bind it. The DLQ is bound to the DLX with a routing key of the original
	 * queue name because we use default exchange routing by queue name for the original message.
	 * @param baseQueueName   The base name for the queue (including the binder prefix, if any).
	 * @param routingKey  The routing key for the queue.
	 * @param properties the properties.
	 */
	private void autoBindDLQ(final String baseQueueName, String routingKey, RabbitCommonProperties properties) {
		boolean autoBindDlq = properties.isAutoBindDlq();
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("autoBindDLQ=" + autoBindDlq
					+ " for: " + baseQueueName);
		}
		if (autoBindDlq) {
			String dlqName;
			if (properties.getDeadLetterQueueName() == null) {
				dlqName = constructDLQName(baseQueueName);
			}
			else {
				dlqName = properties.getDeadLetterQueueName();
			}
			Queue dlq = new Queue(dlqName, true, false, false, queueArgs(dlqName, properties, true));
			declareQueue(dlqName, dlq);
			String dlxName = deadLetterExchangeName(properties);
			if (properties.isDeclareDlx()) {
				declareExchange(dlxName,
						new ExchangeBuilder(dlxName, properties.getDeadLetterExchangeType())
							.durable(true)
							.build());
			}
			Binding dlqBinding = new Binding(dlq.getName(), DestinationType.QUEUE, dlxName,
					properties.getDlqDeadLetterRoutingKey() == null ? routingKey : properties.getDeadLetterRoutingKey(),
					null);
			declareBinding(dlqName, dlqBinding);
			if (properties instanceof RabbitConsumerProperties &&
					((RabbitConsumerProperties) properties).isRepublishToDlq()) {
				/*
				 *  Also bind with the base queue name when republishToDlq is used, which does not know about
				 * partitioning
				 */
				declareBinding(dlqName,
						new Binding(dlq.getName(), DestinationType.QUEUE, dlxName, baseQueueName, null));
			}
		}
	}

	/**
	 * For binder implementations that support dead lettering, construct the name of the
	 * dead letter entity for the underlying pipe name.
	 *
	 * @param name the name.
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


	private void declareQueue(String beanName, Queue queue) {
		try {
			this.rabbitAdmin.declareQueue(queue);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of queue: " + queue.getName() + " deferred - connection not available");
			}
		}
		catch (RuntimeException e) {
			if (this.notOurAdminException) {
				this.notOurAdminException = false;
				throw e;
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of queue: " + queue.getName() + " deferred", e);
			}
		}
		addToAutoDeclareContext(beanName, queue);
	}

	private Map<String, Object> queueArgs(String queueName, RabbitCommonProperties properties, boolean isDlq) {
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
				args.put("x-dead-letter-routing-key", properties.getDlqDeadLetterRoutingKey());
			}
		}
		additionalArgs(args, properties, isDlq);
		return args;
	}

	private void additionalArgs(Map<String, Object> args, RabbitCommonProperties properties, boolean isDlq) {
		Integer expires = isDlq ? properties.getDlqExpires() : properties.getExpires();
		Integer maxLength = isDlq ? properties.getDlqMaxLength() : properties.getMaxLength();
		Integer maxLengthBytes = isDlq ? properties.getDlqMaxLengthBytes() : properties.getMaxLengthBytes();
		Integer maxPriority = isDlq ? properties.getDlqMaxPriority() : properties.getMaxPriority();
		Integer ttl = isDlq ? properties.getDlqTtl() : properties.getTtl();
		boolean lazy = isDlq ? properties.isDlqLazy() : properties.isLazy();
		String overflow = isDlq ? properties.getDlqOverflowBehavior() : properties.getOverflowBehavior();
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
	}


	public static String applyPrefix(String prefix, String name) {
		return prefix + name;
	}

	private Exchange buildExchange(RabbitCommonProperties properties, String exchangeName) {
		try {
			ExchangeBuilder builder = new ExchangeBuilder(exchangeName, properties.getExchangeType());
			builder.durable(properties.isExchangeDurable());
			if (properties.isExchangeAutoDelete()) {
				builder.autoDelete();
			}
			if (properties.isDelayedExchange()) {
				builder.delayed();
			}
			return builder.build();
		}
		catch (Exception e) {
			throw new ProvisioningException("Failed to create exchange object", e);
		}
	}

	private void declareExchange(final String rootName, final Exchange exchange) {
		try {
			this.rabbitAdmin.declareExchange(exchange);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of exchange: " + exchange.getName() + " deferred - connection not available");
			}
		}
		catch (RuntimeException e) {
			if (this.notOurAdminException) {
				this.notOurAdminException = false;
				throw e;
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of exchange: " + exchange.getName() + " deferred", e);
			}
		}
		addToAutoDeclareContext(rootName + ".exchange", exchange);
	}

	private void addToAutoDeclareContext(String name, Object bean) {
		synchronized (this.autoDeclareContext) {
			if (!this.autoDeclareContext.containsBean(name)) {
				this.autoDeclareContext.getBeanFactory().registerSingleton(name, bean);
			}
		}
	}

	private void declareBinding(String rootName, org.springframework.amqp.core.Binding binding) {
		try {
			this.rabbitAdmin.declareBinding(binding);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of binding: " + rootName + ".binding deferred - connection not available");
			}
		}
		catch (RuntimeException e) {
			if (this.notOurAdminException) {
				this.notOurAdminException = false;
				throw e;
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of binding: " + rootName + ".binding deferred", e);
			}
		}
		addToAutoDeclareContext(rootName + ".binding", binding);
	}

	public void cleanAutoDeclareContext(ConsumerDestination destination,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {
		synchronized (this.autoDeclareContext) {
			Stream.of(StringUtils.tokenizeToStringArray(destination.getName(), ",", true, true)).forEach(name -> {
				name = name.trim();
				removeSingleton(name + ".binding");
				removeSingleton(name);
				String dlq = name + ".dlq";
				removeSingleton(dlq + ".binding");
				removeSingleton(dlq);
			});
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
	public void onApplicationEvent(DeclarationExceptionEvent event) {
		this.notOurAdminException = true; // our admin doesn't have an event publisher
	}

	private static final class RabbitProducerDestination implements ProducerDestination {

		private final Exchange exchange;

		private final Binding binding;

		RabbitProducerDestination(Exchange exchange, Binding binding) {
			Assert.notNull(exchange, "exchange must not be null");
			this.exchange = exchange;
			this.binding = binding;
		}

		@Override
		public String getName() {
			return this.exchange.getName();
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.exchange.getName();
		}

		@Override
		public String toString() {
			return "RabbitProducerDestination{" +
					"exchange=" + exchange +
					", binding=" + binding +
					'}';
		}
	}

	private static final class RabbitConsumerDestination implements ConsumerDestination {

		private final String queue;
		private final Binding binding;

		RabbitConsumerDestination(String queue, Binding binding) {
			Assert.notNull(queue, "queue must not be null");
			this.queue = queue;
			this.binding = binding;
		}

		@Override
		public String toString() {
			return "RabbitConsumerDestination{" +
					"queue=" + queue +
					", binding=" + binding +
					'}';
		}

		@Override
		public String getName() {
			return this.queue;
		}
	}

}
