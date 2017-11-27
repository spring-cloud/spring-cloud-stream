/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.support.converter.ConfigurableCompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class ContentTypeConfiguration {

	@Autowired(required = false)
	private ObjectMapper objectMapper;

	/**
	 * User defined custom message converters
	 */
	@Autowired(required = false)
	@StreamMessageConverter
	private List<MessageConverter> customMessageConverters;

	@Bean
	public CompositeMessageConverterFactory compositeMessageConverterFactory() {
		List<MessageConverter> messageConverters = new ArrayList<>();
		if (!CollectionUtils.isEmpty(this.customMessageConverters)) {
			messageConverters.addAll(Collections.unmodifiableCollection(this.customMessageConverters));
		}
		return new CompositeMessageConverterFactory(messageConverters, this.objectMapper);
	}

	@Bean(name = IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME)
	public ConfigurableCompositeMessageConverter configurableCompositeMessageConverter(CompositeMessageConverterFactory factory){
		return new ConfigurableCompositeMessageConverter(factory.getMessageConverterForAllRegistered().getConverters());
	}
}
