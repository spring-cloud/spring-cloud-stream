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

package org.springframework.cloud.stream.schema.avro;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.cloud.stream.schema.registry.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.registry.SchemaReference;
import org.springframework.cloud.stream.schema.registry.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.registry.avro.AvroSchemaRegistryClientMessageConverter;
import org.springframework.cloud.stream.schema.registry.client.SchemaRegistryClient;

/**
 * @author Marius Bogoevici
 */
public class StubSchemaRegistryClient implements SchemaRegistryClient {

	private final AtomicInteger index = new AtomicInteger(0);

	private final Map<Integer, String> schemasById = new HashMap<>();

	private final Map<String, Map<Integer, SchemaWithId>> storedSchemas = new HashMap<>();

	@Override
	public SchemaRegistrationResponse register(String subject, String format,
			String schema) {
		if (!this.storedSchemas.containsKey(subject)) {
			this.storedSchemas.put(subject, new TreeMap<Integer, SchemaWithId>());
		}
		Map<Integer, SchemaWithId> schemaVersions = this.storedSchemas.get(subject);
		for (Map.Entry<Integer, SchemaWithId> integerSchemaEntry : schemaVersions
				.entrySet()) {

			if (integerSchemaEntry.getValue().getSchema().equals(schema)) {
				SchemaRegistrationResponse schemaRegistrationResponse = new SchemaRegistrationResponse();
				schemaRegistrationResponse.setId(integerSchemaEntry.getValue().getId());
				schemaRegistrationResponse.setSchemaReference(
						new SchemaReference(subject, integerSchemaEntry.getKey(),
								AvroSchemaRegistryClientMessageConverter.AVRO_FORMAT));
				return schemaRegistrationResponse;
			}
		}
		int nextVersion = schemaVersions.size() + 1;
		int id = this.index.incrementAndGet();
		schemaVersions.put(nextVersion, new SchemaWithId(id, schema));
		SchemaRegistrationResponse schemaRegistrationResponse = new SchemaRegistrationResponse();
		schemaRegistrationResponse.setId(this.index.getAndIncrement());
		schemaRegistrationResponse.setSchemaReference(new SchemaReference(subject,
				nextVersion, AvroSchemaRegistryClientMessageConverter.AVRO_FORMAT));
		this.schemasById.put(id, schema);
		return schemaRegistrationResponse;
	}

	@Override
	public String fetch(SchemaReference schemaReference) {
		if (!AvroSchemaRegistryClientMessageConverter.AVRO_FORMAT
				.equals(schemaReference.getFormat())) {
			throw new IllegalArgumentException("Only 'avro' is supported by this client");
		}
		if (!this.storedSchemas.containsKey(schemaReference.getSubject())) {
			throw new SchemaNotFoundException("Not found: " + schemaReference);
		}
		if (!this.storedSchemas.get(schemaReference.getSubject())
				.containsKey(schemaReference.getVersion())) {
			throw new SchemaNotFoundException("Not found: " + schemaReference);
		}
		return this.storedSchemas.get(schemaReference.getSubject())
				.get(schemaReference.getVersion()).getSchema();
	}

	@Override
	public String fetch(int id) {
		return this.schemasById.get(id);
	}

	static class SchemaWithId {

		int id;

		String schema;

		SchemaWithId(int id, String schema) {
			this.id = id;
			this.schema = schema;
		}

		public int getId() {
			return this.id;
		}

		public String getSchema() {
			return this.schema;
		}

	}

}
