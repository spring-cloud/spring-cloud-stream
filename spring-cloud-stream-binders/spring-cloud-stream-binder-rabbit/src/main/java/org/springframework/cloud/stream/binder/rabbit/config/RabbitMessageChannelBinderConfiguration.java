/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

/**
 * @author David Turanski
 */

import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.ConnectionFactorySettings;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.config.codec.CodecConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.codec.Codec;

@Configuration
@Import(CodecConfiguration.class)
@EnableConfigurationProperties({RabbitBinderConfigurationProperties.class})
public class RabbitMessageChannelBinderConfiguration {

	@Autowired
	private Codec codec;

	@Autowired
	private ConnectionFactory rabbitConnectionFactory;
	
	@Autowired
	private RabbitBinderConfigurationProperties rabbitBinderConfigurationProperties;
	
	@Autowired
	private SpringRabbitMQProperties springRabbitMQProperties;

	@Bean
	RabbitMessageChannelBinder rabbitMessageChannelBinder() {
		RabbitMessageChannelBinder binder = new RabbitMessageChannelBinder(rabbitConnectionFactory);
		binder.setCodec(codec);
		binder.setAddresses(springRabbitMQProperties.getAddresses());
		binder.setAdminAddresses(springRabbitMQProperties.getAdminAdresses());
		binder.setCompressingPostProcessor(gZipPostProcessor());
		binder.setDecompressingPostProcessor(deCompressingPostProcessor());
		binder.setDefaultAcknowledgeMode(rabbitBinderConfigurationProperties.getAcknowledgeMode());
		binder.setDefaultAutoBindDLQ(rabbitBinderConfigurationProperties.isAutoBindDLQ());
		binder.setDefaultChannelTransacted(rabbitBinderConfigurationProperties.isTransacted());
		binder.setDefaultDefaultDeliveryMode(rabbitBinderConfigurationProperties.getDefaultDeliveryMode());
		binder.setDefaultDefaultRequeueRejected(rabbitBinderConfigurationProperties.isDefaultRequeueRejected());
		binder.setDefaultMaxConcurrency(rabbitBinderConfigurationProperties.getMaxConcurrency());
		binder.setDefaultPrefetchCount(rabbitBinderConfigurationProperties.getPrefetchCount());
		binder.setDefaultPrefix(rabbitBinderConfigurationProperties.getPrefix());
		binder.setDefaultReplyHeaderPatterns(rabbitBinderConfigurationProperties.getReplyHeaderPatterns());
		binder.setDefaultRepublishToDLQ(rabbitBinderConfigurationProperties.isRepublishToDLQ());
		binder.setDefaultRequestHeaderPatterns(rabbitBinderConfigurationProperties.getRequestHeaderPatterns());
		binder.setDefaultTxSize(rabbitBinderConfigurationProperties.getTxSize());
		binder.setNodes(springRabbitMQProperties.getNodes());
		binder.setPassword(springRabbitMQProperties.getPassword());
		binder.setSslPropertiesLocation(springRabbitMQProperties.getSslPropertiesLocation());
		binder.setUsername(springRabbitMQProperties.getUsername());
		binder.setUseSSL(springRabbitMQProperties.isUseSSL());
		binder.setVhost(springRabbitMQProperties.getVhost());
		return binder;
	}
	
	@Bean
	MessagePostProcessor deCompressingPostProcessor() {
		return new DelegatingDecompressingPostProcessor();
	}
	
	@Bean
	MessagePostProcessor gZipPostProcessor() {
		GZipPostProcessor gZipPostProcessor = new GZipPostProcessor();
		gZipPostProcessor.setLevel(rabbitBinderConfigurationProperties.getCompressionLevel());
		return  gZipPostProcessor;
	}
	

	@Bean
	ConnectionFactorySettings rabbitConnectionFactorySettings() {
		return new ConnectionFactorySettings();
	}

	@Bean
	SpringRabbitMQProperties springRabbitMQProperties() {
		return new SpringRabbitMQProperties();
	}
}
