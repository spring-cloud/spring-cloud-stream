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

package org.springframework.cloud.stream.schema.registry.avro;

import java.lang.reflect.Constructor;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cloud.stream.schema.registry.client.SchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 * @author Sercan Karaoglu
 * @author Ish Mahajan
 * @author Christian Tzolov
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(name = "org.apache.avro.Schema")
@ConditionalOnProperty(value = "spring.cloud.stream.schemaRegistryClient.enabled", matchIfMissing = true)
@ConditionalOnBean(SchemaRegistryClient.class)
@EnableConfigurationProperties({ AvroMessageConverterProperties.class })
@Import(AvroSchemaServiceManagerImpl.class)
public class AvroMessageConverterAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean(AvroSchemaRegistryClientMessageConverter.class)
	public AvroSchemaRegistryClientMessageConverter avroSchemaMessageConverter1(
			SchemaRegistryClient schemaRegistryClient,
			AvroSchemaServiceManager avroSchemaServiceManager,
			AvroMessageConverterProperties avroMessageConverterProperties) {

		AvroSchemaRegistryClientMessageConverter avroSchemaRegistryClientMessageConverter =
				new AvroSchemaRegistryClientMessageConverter(schemaRegistryClient, cacheManager(), avroSchemaServiceManager);

		avroSchemaRegistryClientMessageConverter.setDynamicSchemaGenerationEnabled(
				avroMessageConverterProperties.isDynamicSchemaGenerationEnabled());

		if (avroMessageConverterProperties.getReaderSchema() != null) {
			avroSchemaRegistryClientMessageConverter.setReaderSchema(avroMessageConverterProperties.getReaderSchema());
		}
		if (!ObjectUtils.isEmpty(avroMessageConverterProperties.getSchemaLocations())) {
			avroSchemaRegistryClientMessageConverter.setSchemaLocations(avroMessageConverterProperties.getSchemaLocations());
		}
		if (!ObjectUtils.isEmpty(avroMessageConverterProperties.getSchemaImports())) {
			avroSchemaRegistryClientMessageConverter.setSchemaImports(avroMessageConverterProperties.getSchemaImports());
		}
		avroSchemaRegistryClientMessageConverter.setPrefix(avroMessageConverterProperties.getPrefix());

		if (avroMessageConverterProperties.isIgnoreSchemaRegistryServer()) {
			avroSchemaRegistryClientMessageConverter.setIgnoreSchemaRegistryServer(true);
		}

		try {
			Class<?> clazz = avroMessageConverterProperties.getSubjectNamingStrategy();
			Constructor constructor = ReflectionUtils.accessibleConstructor(clazz);
			avroSchemaRegistryClientMessageConverter.setSubjectNamingStrategy(
					(SubjectNamingStrategy) constructor.newInstance());
		}
		catch (Exception ex) {
			throw new IllegalStateException("Unable to create SubjectNamingStrategy "
					+ avroMessageConverterProperties.getSubjectNamingStrategy().toString(), ex);
		}
		avroSchemaRegistryClientMessageConverter.setSubjectNamePrefix(avroMessageConverterProperties.getSubjectNamePrefix());

		return avroSchemaRegistryClientMessageConverter;
	}

	@Bean
	@ConditionalOnMissingBean
	public CacheManager cacheManager() {
		return new ConcurrentMapCacheManager();
	}

}
