/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.pulsar.PulsarMessageChannelBinder;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarExtendedBindingProperties;
import org.springframework.cloud.stream.binder.pulsar.provisioning.PulsarTopicProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.autoconfigure.PulsarProperties;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.support.header.JacksonUtils;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;
import org.springframework.pulsar.support.header.ToStringPulsarHeaderMapper;

/**
 * Pulsar binder {@link Configuration}.
 *
 * @author Soby Chacko
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(Binder.class)
@EnableConfigurationProperties({ PulsarProperties.class, PulsarExtendedBindingProperties.class,
		PulsarBinderConfigurationProperties.class })
public class PulsarBinderConfiguration {

	@Bean
	public PulsarTopicProvisioner pulsarTopicProvisioner(PulsarAdministration pulsarAdministration,
														PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties) {
		return new PulsarTopicProvisioner(pulsarAdministration, pulsarBinderConfigurationProperties);
	}

	@Bean
	@ConditionalOnMissingBean
	public PulsarHeaderMapper pulsarHeaderMapper() {
		if (JacksonUtils.isJacksonPresent()) {
			return JsonPulsarHeaderMapper.builder().build();
		}
		return new ToStringPulsarHeaderMapper();
	}

	@Bean
	public PulsarMessageChannelBinder pulsarMessageChannelBinder(PulsarTopicProvisioner pulsarTopicProvisioner,
															PulsarTemplate<Object> pulsarTemplate, PulsarConsumerFactory<byte[]> pulsarConsumerFactory,
															PulsarBinderConfigurationProperties binderConfigProps, PulsarExtendedBindingProperties bindingConfigProps,
															SchemaResolver schemaResolver, PulsarHeaderMapper headerMapper) {
		PulsarMessageChannelBinder pulsarMessageChannelBinder = new PulsarMessageChannelBinder(pulsarTopicProvisioner,
				pulsarTemplate, pulsarConsumerFactory, binderConfigProps, schemaResolver, headerMapper);
		pulsarMessageChannelBinder.setExtendedBindingProperties(bindingConfigProps);
		return pulsarMessageChannelBinder;
	}

}
