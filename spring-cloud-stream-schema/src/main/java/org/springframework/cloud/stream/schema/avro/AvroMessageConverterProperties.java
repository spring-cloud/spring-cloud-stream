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

package org.springframework.cloud.stream.schema.avro;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * @author Vinicius Carvalho
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.schema.avro")
public class AvroMessageConverterProperties {

	private boolean dynamicSchemaGenerationEnabled;

	private Resource readerSchema;

	private Resource[] schemaLocations;

	private String prefix = "vnd";

	private Class<? extends SubjectNamingStrategy> subjectNamingStrategy = DefaultSubjectNamingStrategy.class;

	public Resource getReaderSchema() {
		return this.readerSchema;
	}

	public void setReaderSchema(Resource readerSchema) {
		Assert.notNull(readerSchema, "cannot be null");
		this.readerSchema = readerSchema;
	}

	public Resource[] getSchemaLocations() {
		return this.schemaLocations;
	}

	public void setSchemaLocations(Resource[] schemaLocations) {
		Assert.notEmpty(schemaLocations, "cannot be null");
		this.schemaLocations = schemaLocations;
	}

	public boolean isDynamicSchemaGenerationEnabled() {
		return this.dynamicSchemaGenerationEnabled;
	}

	public void setDynamicSchemaGenerationEnabled(boolean dynamicSchemaGenerationEnabled) {
		this.dynamicSchemaGenerationEnabled = dynamicSchemaGenerationEnabled;
	}

	public String getPrefix() {
		return this.prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public Class<?> getSubjectNamingStrategy() {
		return subjectNamingStrategy;
	}

	public void setSubjectNamingStrategy(Class<? extends SubjectNamingStrategy>  subjectNamingStrategy) {
		Assert.notNull(subjectNamingStrategy, "cannot be null");
		this.subjectNamingStrategy = subjectNamingStrategy;
	}
}
