/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.support;

import java.util.List;

import org.springframework.cloud.stream.schema.registry.model.Compatibility;
import org.springframework.cloud.stream.schema.registry.model.Schema;

/**
 * @author Vinicius Carvalho
 * @author Christian Tzolov
 *
 * Provides utility methods to validate, check compatibility and match schemas of
 * different implementations
 */
public interface SchemaValidator {

	/**
	 * Verifies if a definition is a valid schema.
	 * @param definition - The textual representation of the schema file
	 * @return true if valid, false otherwise
	 */
	boolean isValid(String definition);

	/**
	 * Validates a schema definition and throws an {@link InvalidSchemaException} when the schema is invalid.
	 * The exception is expected to have the violation description.
	 * @param definition - The textual representation of the schema file
	 */
	default void validate(String definition) {
		if (!this.isValid(definition)) {
			throw new InvalidSchemaException("Invalid Schema");
		}
	}

	/**
	 * Checks for compatibility between two schemas @see Compatibility class for types
	 * This method may not be supported for certain formats.
	 * @param source - The textual representation of the schema to tested
	 * @param other - The textual representation of the other schema to tested
	 * @return {@link Compatibility}
	 */
	Compatibility compatibilityCheck(String source, String other);

	/**
	 * Return the Schema that is represented by the definition.
	 * @param schemas List of schemas to be tested
	 * @param definition Textual representation of the schema
	 * @return A full Schema object with identifier and subject properties
	 */
	Schema match(List<Schema> schemas, String definition);

	String getFormat();

}
