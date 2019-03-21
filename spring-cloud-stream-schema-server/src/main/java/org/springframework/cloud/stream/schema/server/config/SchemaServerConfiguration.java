/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.schema.server.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.domain.EntityScanPackages;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.schema.server.controllers.ServerController;
import org.springframework.cloud.stream.schema.server.model.Schema;
import org.springframework.cloud.stream.schema.server.repository.SchemaRepository;
import org.springframework.cloud.stream.schema.server.support.AvroSchemaValidator;
import org.springframework.cloud.stream.schema.server.support.SchemaValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * @author Vinicius Carvalho
 * @author Soby Chacko
 */
@Configuration
@EnableJpaRepositories(basePackageClasses = SchemaRepository.class)
@EnableConfigurationProperties(SchemaServerProperties.class)
@Import(ServerController.class)
public class SchemaServerConfiguration {

	@Bean
	public static BeanFactoryPostProcessor entityScanPackagesPostProcessor() {
		return new BeanFactoryPostProcessor() {

			@Override
			public void postProcessBeanFactory(
					ConfigurableListableBeanFactory beanFactory) throws BeansException {
				if (beanFactory instanceof BeanDefinitionRegistry) {
					EntityScanPackages.register((BeanDefinitionRegistry) beanFactory,
							Collections
									.singletonList(Schema.class.getPackage().getName()));
				}
			}
		};
	}

	@Bean
	public Map<String, SchemaValidator> schemaValidators() {
		Map<String, SchemaValidator> validatorMap = new HashMap<>();
		validatorMap.put("avro", new AvroSchemaValidator());
		return validatorMap;
	}

}
