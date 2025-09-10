/*
 * Copyright 2015-present the original author or authors.
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

import java.util.stream.Collectors;

import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.CredentialsRefreshService;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.amqp.autoconfigure.CachingConnectionFactoryConfigurer;
import org.springframework.boot.amqp.autoconfigure.ConnectionFactoryCustomizer;
import org.springframework.boot.amqp.autoconfigure.RabbitConnectionFactoryBeanConfigurer;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.amqp.autoconfigure.RabbitRetryTemplateCustomizer;
import org.springframework.boot.amqp.autoconfigure.RabbitTemplateConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;

/**
 * Configuration for {@link RabbitTemplate} and {@link CachingConnectionFactory}.
 *
 * @author Chris Bono
 * @since 3.2
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RabbitProperties.class)
public class RabbitConfiguration {

	@Bean
	@ConditionalOnMissingBean
	RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer(RabbitProperties properties,
			ResourceLoader resourceLoader, ObjectProvider<CredentialsProvider> credentialsProvider,
			ObjectProvider<CredentialsRefreshService> credentialsRefreshService) {
		RabbitConnectionFactoryBeanConfigurer configurer = new RabbitConnectionFactoryBeanConfigurer(resourceLoader, properties);
		configurer.setCredentialsProvider(credentialsProvider.getIfUnique());
		configurer.setCredentialsRefreshService(credentialsRefreshService.getIfUnique());
		return configurer;
	}

	@Bean
	@ConditionalOnMissingBean
	CachingConnectionFactoryConfigurer rabbitConnectionFactoryConfigurer(RabbitProperties rabbitProperties,
			ObjectProvider<ConnectionNameStrategy> connectionNameStrategy) {
		CachingConnectionFactoryConfigurer configurer = new CachingConnectionFactoryConfigurer(rabbitProperties);
		configurer.setConnectionNameStrategy(connectionNameStrategy.getIfUnique());
		return configurer;
	}

	@Bean
	@ConditionalOnMissingBean(ConnectionFactory.class)
	CachingConnectionFactory rabbitConnectionFactory(
			RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer,
			CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer,
			ObjectProvider<ConnectionFactoryCustomizer> connectionFactoryCustomizers) throws Exception {

		RabbitConnectionFactoryBean connectionFactoryBean = new RabbitConnectionFactoryBean();
		rabbitConnectionFactoryBeanConfigurer.configure(connectionFactoryBean);
		connectionFactoryBean.afterPropertiesSet();
		com.rabbitmq.client.ConnectionFactory connectionFactory = connectionFactoryBean.getObject();
		connectionFactoryCustomizers.orderedStream()
				.forEach((customizer) -> customizer.customize(connectionFactory));

		CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(connectionFactory);
		rabbitCachingConnectionFactoryConfigurer.configure(cachingConnectionFactory);

		return cachingConnectionFactory;
	}

	@Bean
	@ConditionalOnMissingBean
	public RabbitTemplateConfigurer rabbitTemplateConfigurer(RabbitProperties properties,
			ObjectProvider<MessageConverter> messageConverter,
			ObjectProvider<RabbitRetryTemplateCustomizer> retryTemplateCustomizers) {
		RabbitTemplateConfigurer configurer = new RabbitTemplateConfigurer(properties);
		configurer.setMessageConverter(messageConverter.getIfUnique());
		configurer.setRetryTemplateCustomizers(retryTemplateCustomizers.orderedStream().collect(Collectors.toList()));
		return configurer;
	}

	@Bean
	@ConditionalOnSingleCandidate(ConnectionFactory.class)
	@ConditionalOnMissingBean(RabbitOperations.class)
	public RabbitTemplate rabbitTemplate(RabbitTemplateConfigurer configurer, ConnectionFactory connectionFactory) {
		RabbitTemplate template = new RabbitTemplate();
		configurer.configure(template, connectionFactory);
		return template;
	}

}
