/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.StreamsConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kstream.KStreamBinder;
import org.springframework.cloud.stream.binder.kstream.MessageConversionDelegate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Soby Chacko
 */
@Configuration
@EnableConfigurationProperties(KStreamExtendedBindingProperties.class)
public class KStreamBinderConfiguration {

	private static final Log logger = LogFactory.getLog(KStreamBinderConfiguration.class);

	@Autowired
	private KafkaProperties kafkaProperties;

	@Bean
	public KafkaTopicProvisioner provisioningProvider(KafkaBinderConfigurationProperties binderConfigurationProperties) {
		return new KafkaTopicProvisioner(binderConfigurationProperties, kafkaProperties);
	}

	@Bean
	public KStreamBinder kStreamBinder(KafkaBinderConfigurationProperties binderConfigurationProperties,
									KafkaTopicProvisioner kafkaTopicProvisioner,
									KStreamExtendedBindingProperties kStreamExtendedBindingProperties, StreamsConfig streamsConfig,
									MessageConversionDelegate messageConversionDelegate) {
		KStreamBinder kStreamBinder = new KStreamBinder(binderConfigurationProperties, kafkaTopicProvisioner, kStreamExtendedBindingProperties,
				streamsConfig, messageConversionDelegate);
		return kStreamBinder;
	}

}
