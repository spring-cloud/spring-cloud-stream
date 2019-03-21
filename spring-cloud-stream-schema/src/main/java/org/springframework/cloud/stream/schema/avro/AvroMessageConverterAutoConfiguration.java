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

package org.springframework.cloud.stream.schema.avro;

import java.lang.reflect.Constructor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cloud.stream.annotation.StreamMessageConverter;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Sercan Karaoglu
 */
@Configuration
@ConditionalOnClass(name = "org.apache.avro.Schema")
@ConditionalOnProperty(value = "spring.cloud.stream.schemaRegistryClient.enabled", matchIfMissing = true)
@ConditionalOnBean(type = "org.springframework.cloud.stream.schema.client.SchemaRegistryClient")
@EnableConfigurationProperties({ AvroMessageConverterProperties.class })
public class AvroMessageConverterAutoConfiguration {

	@Autowired
	private AvroMessageConverterProperties avroMessageConverterProperties;

	@Bean
	@ConditionalOnMissingBean(AvroSchemaRegistryClientMessageConverter.class)
	@StreamMessageConverter
	public AvroSchemaRegistryClientMessageConverter avroSchemaMessageConverter(
			SchemaRegistryClient schemaRegistryClient) {
		AvroSchemaRegistryClientMessageConverter avroSchemaRegistryClientMessageConverter;
		avroSchemaRegistryClientMessageConverter = new AvroSchemaRegistryClientMessageConverter(
				schemaRegistryClient, cacheManager());
		avroSchemaRegistryClientMessageConverter.setDynamicSchemaGenerationEnabled(
				this.avroMessageConverterProperties.isDynamicSchemaGenerationEnabled());
		if (this.avroMessageConverterProperties.getReaderSchema() != null) {
			avroSchemaRegistryClientMessageConverter.setReaderSchema(
					this.avroMessageConverterProperties.getReaderSchema());
		}
		if (!ObjectUtils
				.isEmpty(this.avroMessageConverterProperties.getSchemaLocations())) {
			avroSchemaRegistryClientMessageConverter.setSchemaLocations(
					this.avroMessageConverterProperties.getSchemaLocations());
		}
		if (!ObjectUtils
				.isEmpty(this.avroMessageConverterProperties.getSchemaImports())) {
			avroSchemaRegistryClientMessageConverter.setSchemaImports(
					this.avroMessageConverterProperties.getSchemaImports());
		}
		avroSchemaRegistryClientMessageConverter
				.setPrefix(this.avroMessageConverterProperties.getPrefix());

		try {
			Class<?> clazz = this.avroMessageConverterProperties
					.getSubjectNamingStrategy();
			Constructor constructor = ReflectionUtils.accessibleConstructor(clazz);

			avroSchemaRegistryClientMessageConverter.setSubjectNamingStrategy(
					(SubjectNamingStrategy) constructor.newInstance());
		}
		catch (Exception ex) {
			throw new IllegalStateException("Unable to create SubjectNamingStrategy "
					+ this.avroMessageConverterProperties.getSubjectNamingStrategy()
							.toString(),
					ex);
		}

		return avroSchemaRegistryClientMessageConverter;
	}

	@Bean
	@ConditionalOnMissingBean
	public CacheManager cacheManager() {
		return new ConcurrentMapCacheManager();
	}

}
