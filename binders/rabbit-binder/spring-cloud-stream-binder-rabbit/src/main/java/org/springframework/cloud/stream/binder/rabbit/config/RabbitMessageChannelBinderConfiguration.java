/*
 * Copyright 2015-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.core.DeclarableCustomizer;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.config.ConsumerEndpointCustomizer;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.inbound.AmqpMessageSource;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.CollectionUtils;


/**
 * Configuration class for RabbitMQ message channel binder.
 *
 * @author David Turanski
 * @author Vinicius Carvalho
 * @author Artem Bilan
 * @author Oleg Zhurakousky
 * @author Gary Russell
 * @author Ben Blinebury
 */
@Configuration(proxyBeanMethods = false)
@Import({ PropertyPlaceholderAutoConfiguration.class })
@EnableConfigurationProperties({ RabbitBinderConfigurationProperties.class,
		RabbitExtendedBindingProperties.class })
public class RabbitMessageChannelBinderConfiguration {

	@Autowired
	private ConnectionFactory rabbitConnectionFactory;

	@Autowired
	private RabbitProperties rabbitProperties;

	@Autowired
	private RabbitBinderConfigurationProperties rabbitBinderConfigurationProperties;

	@Autowired
	private RabbitExtendedBindingProperties rabbitExtendedBindingProperties;

	@Bean
	RabbitMessageChannelBinder rabbitMessageChannelBinder(
			@Nullable List<ListenerContainerCustomizer<MessageListenerContainer>> listenerContainerCustomizers,
			@Nullable MessageSourceCustomizer<AmqpMessageSource> sourceCustomizer,
			@Nullable List<ProducerMessageHandlerCustomizer<MessageHandler>> producerMessageHandlerCustomizers,
			@Nullable ConsumerEndpointCustomizer<AmqpInboundChannelAdapter> consumerCustomizer,
			List<DeclarableCustomizer> declarableCustomizers,
			@Nullable ConnectionNameStrategy connectionNameStrategy) {

		String connectionNamePrefix = this.rabbitBinderConfigurationProperties.getConnectionNamePrefix();
		if (this.rabbitConnectionFactory instanceof AbstractConnectionFactory connectionFactory
			&& connectionNamePrefix != null &&  connectionNameStrategy == null) {
			final AtomicInteger nameIncrementer = new AtomicInteger();
			connectionFactory.setConnectionNameStrategy(f -> connectionNamePrefix
					+ "#" + nameIncrementer.getAndIncrement());
		}

		ListenerContainerCustomizer<MessageListenerContainer> composedCistomizer = new ListenerContainerCustomizer<>() {
			@Override
			public void configure(MessageListenerContainer container, String destinationName, String group) {
				if (!CollectionUtils.isEmpty(listenerContainerCustomizers)) {
					for (ListenerContainerCustomizer<MessageListenerContainer> customizer : listenerContainerCustomizers) {
						customizer.configure(container, destinationName, group);
					}
				}
			}
		};
		ProducerMessageHandlerCustomizer<MessageHandler> producerMessageHandlerCustomizer = new ProducerMessageHandlerCustomizer<>() {
			@Override
			public void configure(MessageHandler handler, String destinationName) {
				if (!CollectionUtils.isEmpty(producerMessageHandlerCustomizers)) {
					for (ProducerMessageHandlerCustomizer<MessageHandler> customizer : producerMessageHandlerCustomizers) {
						customizer.configure(handler, destinationName);
					}
				}
			}
		};

		RabbitMessageChannelBinder binder = new RabbitMessageChannelBinder(
				this.rabbitConnectionFactory, this.rabbitProperties,
				provisioningProvider(declarableCustomizers), composedCistomizer, sourceCustomizer);
		binder.setAdminAddresses(
				this.rabbitBinderConfigurationProperties.getAdminAddresses());
		binder.setCompressingPostProcessor(gZipPostProcessor());
		binder.setDecompressingPostProcessor(deCompressingPostProcessor());
		binder.setNodes(this.rabbitBinderConfigurationProperties.getNodes());
		binder.setExtendedBindingProperties(this.rabbitExtendedBindingProperties);
		binder.setProducerMessageHandlerCustomizer(producerMessageHandlerCustomizer);
		binder.setConsumerEndpointCustomizer(consumerCustomizer);
		return binder;
	}

	@Bean
	MessagePostProcessor deCompressingPostProcessor() {
		return new DelegatingDecompressingPostProcessor();
	}

	@Bean
	MessagePostProcessor gZipPostProcessor() {
		GZipPostProcessor gZipPostProcessor = new GZipPostProcessor();

		if (this.rabbitBinderConfigurationProperties.getCompressionLevel() != null) {
			gZipPostProcessor.setLevel(this.rabbitBinderConfigurationProperties.getCompressionLevel());
		}

		return gZipPostProcessor;
	}

	@Bean
	RabbitExchangeQueueProvisioner provisioningProvider(List<DeclarableCustomizer> customizers) {
		return new RabbitExchangeQueueProvisioner(this.rabbitConnectionFactory, customizers);
	}
}
