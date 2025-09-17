/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.schema.registry;

import org.apache.avro.Schema;

/**
 * Stores a {@link Schema} together with its String representation.
 *
 * Helps to avoid unnecessary parsing of schema textual representation, as well as calls
 * to {@link org.apache.avro.Schema} toString method which is very expensive due the
 * utilization of {@link tools.jackson.databind.ObjectMapper} to output a JSON
 * representation of the schema.
 *
 * Once a schema is found for any Class, be it a POJO or a
 * {@link org.apache.avro.generic.GenericContainer}, both textual representation as well
 * as the {@link org.apache.avro.Schema} will be stored within this class.
 *
 * @author Vinicius Carvalho
 *
 */
public class ParsedSchema {

	private final Schema schema;

	private final String representation;

	private SchemaRegistrationResponse registration;

	public ParsedSchema(Schema schema) {
		this.schema = schema;
		this.representation = schema.toString();
	}

	public Schema getSchema() {
		return this.schema;
	}

	public String getRepresentation() {
		return this.representation;
	}

	public SchemaRegistrationResponse getRegistration() {
		return this.registration;
	}

	public void setRegistration(SchemaRegistrationResponse registration) {
		this.registration = registration;
	}

}
