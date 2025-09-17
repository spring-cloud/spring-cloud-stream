/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.persistence.autoconfigure.EntityScanPackages;
import org.springframework.cloud.stream.schema.registry.controllers.ServerController;
import org.springframework.cloud.stream.schema.registry.model.Schema;
import org.springframework.cloud.stream.schema.registry.repository.SchemaRepository;
import org.springframework.cloud.stream.schema.registry.support.AvroSchemaValidator;
import org.springframework.cloud.stream.schema.registry.support.SchemaValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * @author Vinicius Carvalho
 * @author Soby Chacko
 * @author Byungjun You
 */
@Configuration(proxyBeanMethods = false)
@EnableJpaRepositories(basePackageClasses = SchemaRepository.class)
@EnableConfigurationProperties(SchemaServerProperties.class)
@Import(ServerController.class)
public class SchemaServerConfiguration {

	@Bean
	public static BeanFactoryPostProcessor entityScanPackagesPostProcessor() {
		return beanFactory -> {
			if (beanFactory instanceof BeanDefinitionRegistry beanDefinitionRegistry) {
				EntityScanPackages.register(beanDefinitionRegistry,
						Collections.singletonList(Schema.class.getPackage().getName()));
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
