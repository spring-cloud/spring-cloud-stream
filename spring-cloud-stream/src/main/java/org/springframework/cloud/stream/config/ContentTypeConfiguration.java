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

package org.springframework.cloud.stream.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.ConfigurableApplicationContext;
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
public class ContentTypeConfiguration implements InitializingBean {

	@Autowired(required = false)
	private ObjectMapper objectMapper;

	@Autowired
	private ConfigurableApplicationContext context;

	/**
	 * User defined custom message converters
	 */
	@Autowired(required = false)
	@StreamMessageConverter
	private List<MessageConverter> customMessageConverters;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (context.getBeanFactory().containsBean(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME)) {
			BeanDefinitionRegistry beanDefinitionRegistry =
					(BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();
			beanDefinitionRegistry.removeBeanDefinition(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME);
		}
	}

	@Bean
	@SuppressWarnings("unchecked")
	public CompositeMessageConverterFactory compositeMessageConverterFactory() {
		List<MessageConverter> messageConverters = new ArrayList<>();
		if (!CollectionUtils.isEmpty(this.customMessageConverters)) {
			messageConverters.addAll(Collections.unmodifiableCollection(this.customMessageConverters));
		}
		CompositeMessageConverterFactory compositeMessageConverterFactory = new CompositeMessageConverterFactory(messageConverters, this.objectMapper);

		// Manually register a bean named as `integrationArgumentResolverMessageConverter`
		// in order to avoid bean name overriding exceptions. This name exists in Spring Integration.
		// The afterProperties method should have removed the bean from the registry and then
		// we are re-registering it again through a different bean definition.
		BeanDefinitionRegistry beanDefinitionRegistry =
				(BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

		ConfigurableCompositeMessageConverter configurableCompositeMessageConverter =
				new ConfigurableCompositeMessageConverter(compositeMessageConverterFactory.getMessageConverterForAllRegistered().getConverters());

		BeanDefinition configurableCompositeMessageConverterDefn =
				BeanDefinitionBuilder.genericBeanDefinition((Class<ConfigurableCompositeMessageConverter>) configurableCompositeMessageConverter.getClass(),
						() -> configurableCompositeMessageConverter)
						.getRawBeanDefinition();

		beanDefinitionRegistry.registerBeanDefinition(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME, configurableCompositeMessageConverterDefn);

		return compositeMessageConverterFactory;
	}

}
