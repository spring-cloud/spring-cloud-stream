/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.schema.avro;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.StringConvertingContentTypeResolver;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 */
@Configuration
@ConditionalOnClass(name = "org.apache.avro.Schema")
@ConditionalOnProperty(value = "spring.cloud.stream.schemaRegistryClient.enabled", matchIfMissing = true)
@ConditionalOnBean(type = "org.springframework.cloud.stream.schema.client.SchemaRegistryClient")
@EnableConfigurationProperties(AvroMessageConverterProperties.class)
public class AvroMessageConverterAutoConfiguration {

	@Autowired
	private AvroMessageConverterProperties avroMessageConverterProperties;

	@Bean
	@ConditionalOnMissingBean(AvroSchemaRegistryClientMessageConverter.class)
	public AvroSchemaRegistryClientMessageConverter avroSchemaMessageConverter(
			SchemaRegistryClient schemaRegistryClient) {
		AvroSchemaRegistryClientMessageConverter
				avroSchemaRegistryClientMessageConverter = new AvroSchemaRegistryClientMessageConverter(
				schemaRegistryClient);
		avroSchemaRegistryClientMessageConverter.setDynamicSchemaGenerationEnabled(
				this.avroMessageConverterProperties.isDynamicSchemaGenerationEnabled());
		avroSchemaRegistryClientMessageConverter.setContentTypeResolver(new StringConvertingContentTypeResolver());
		if (this.avroMessageConverterProperties.getReaderSchema() != null) {
			avroSchemaRegistryClientMessageConverter.setReaderSchema(
					this.avroMessageConverterProperties.getReaderSchema());
		}
		if (!ObjectUtils.isEmpty(this.avroMessageConverterProperties.getSchemaLocations())) {
			avroSchemaRegistryClientMessageConverter.setSchemaLocations(
					this.avroMessageConverterProperties.getSchemaLocations());
		}
		avroSchemaRegistryClientMessageConverter.setPrefix(this.avroMessageConverterProperties.getPrefix());
		return avroSchemaRegistryClientMessageConverter;
	}
}
