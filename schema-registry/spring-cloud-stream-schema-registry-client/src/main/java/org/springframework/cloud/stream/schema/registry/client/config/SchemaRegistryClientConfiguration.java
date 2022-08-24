/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.client.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.stream.schema.registry.client.CachingRegistryClient;
import org.springframework.cloud.stream.schema.registry.client.DefaultSchemaRegistryClient;
import org.springframework.cloud.stream.schema.registry.client.SchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Soby Chacko
 */
@Configuration
@EnableConfigurationProperties(SchemaRegistryClientProperties.class)
public class SchemaRegistryClientConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public SchemaRegistryClient schemaRegistryClient(SchemaRegistryClientProperties schemaRegistryClientProperties,
			RestTemplateBuilder restTemplateBuilder) {
		DefaultSchemaRegistryClient defaultSchemaRegistryClient = new DefaultSchemaRegistryClient(restTemplateBuilder);

		if (StringUtils.hasText(schemaRegistryClientProperties.getEndpoint())) {
			defaultSchemaRegistryClient.setEndpoint(schemaRegistryClientProperties.getEndpoint());
		}

		SchemaRegistryClient client = (schemaRegistryClientProperties.isCached())
				? new CachingRegistryClient(defaultSchemaRegistryClient)
				: defaultSchemaRegistryClient;

		return client;
	}

}
