/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.schema.server.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.schema.server.controllers.ServerController;
import org.springframework.cloud.stream.schema.server.repository.SchemaRepository;
import org.springframework.cloud.stream.schema.server.support.AvroSchemaValidator;
import org.springframework.cloud.stream.schema.server.support.SchemaValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@EnableJpaRepositories(basePackageClasses = SchemaRepository.class)
@EnableConfigurationProperties(SchemaServerProperties.class)
public class SchemaServerConfiguration {

	@Bean
	public ServerController serverController(SchemaRepository repository) {
		return new ServerController(repository, schemaValidators());
	}

	@Bean
	public Map<String, SchemaValidator> schemaValidators() {
		Map<String, SchemaValidator> validatorMap = new HashMap<>();
		validatorMap.put("avro", new AvroSchemaValidator());
		return validatorMap;
	}
}
