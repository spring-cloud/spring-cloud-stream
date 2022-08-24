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

package org.springframework.cloud.stream.schema.registry.client;

import org.springframework.cloud.stream.schema.registry.SchemaReference;
import org.springframework.cloud.stream.schema.registry.SchemaRegistrationResponse;

/**
 * @author Vinicius Carvalho
 * @author Marius Bogoevici
 */
public interface SchemaRegistryClient {

	/**
	 * Registers a schema with the remote repository returning the unique identifier
	 * associated with this schema.
	 * @param subject the full name of the schema
	 * @param format format of the schema
	 * @param schema string representation of the schema
	 * @return a {@link SchemaRegistrationResponse} representing the result of the
	 * operation
	 */
	SchemaRegistrationResponse register(String subject, String format, String schema);

	/**
	 * Retrieves a schema by its reference (subject and version).
	 * @param schemaReference a {@link SchemaReference} used to identify the target
	 * schema.
	 * @return schema
	 */
	String fetch(SchemaReference schemaReference);

	/**
	 * Retrieves a schema by its identifier.
	 * @param id the id of the target schema.
	 * @return schema
	 */
	String fetch(int id);

}
