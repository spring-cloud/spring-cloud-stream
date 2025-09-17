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

package org.springframework.cloud.stream.schema.registry.client;

import tools.jackson.core.JsonProcessingException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.cloud.stream.schema.registry.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.registry.SchemaReference;
import org.springframework.cloud.stream.schema.registry.SchemaRegistrationResponse;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Vinicius Carvalho
 * @author Marius Bogoevici
 * @author Jon Archer
 * @author Tengzhou Dong
 */
public class ConfluentSchemaRegistryClient implements SchemaRegistryClient {

	private static final List<String> ACCEPT_HEADERS = Arrays.asList(
			"application/vnd.schemaregistry.v1+json",
			"application/vnd.schemaregistry+json", "application/json");

	private RestTemplate template;

	private String endpoint = "http://localhost:8081";

	private ObjectMapper mapper;

	public ConfluentSchemaRegistryClient() {
		this(new RestTemplate());
	}

	public ConfluentSchemaRegistryClient(RestTemplate template) {
		this(template, new ObjectMapper());
	}

	public ConfluentSchemaRegistryClient(RestTemplate template, ObjectMapper mapper) {
		this.template = template;
		this.mapper = mapper;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public SchemaRegistrationResponse register(String subject, String format, String schema) {
		Assert.isTrue("avro".equals(format), "Only Avro is supported");
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept", ACCEPT_HEADERS);
		headers.add("Content-Type", "application/json");
		Integer version = null;
		Integer id = null;
		String payload = null;
		Map<String, String> maps = new HashMap<>();
		maps.put("schema", schema);
		try {
			payload = this.mapper.writeValueAsString(maps);
		}
		catch (JsonProcessingException e) {
			throw new RuntimeException("Could not parse schema, invalid JSON format", e);
		}
		try {
			HttpEntity<String> request = new HttpEntity<>(payload, headers);
			ResponseEntity<Map> response = this.template.exchange(
					this.endpoint + "/subjects/" + subject + "/versions", HttpMethod.POST, request, Map.class);
			id = (Integer) response.getBody().get("id");
		}
		catch (HttpStatusCodeException httpException) {
			throw new RuntimeException(String.format("Failed to register subject %s, server replied with status %d",
					subject, httpException.getStatusCode().value()), httpException);
		}

		try {
			ResponseEntity<List> response = this.template.getForEntity(
					this.endpoint + "/schemas/ids/" + id + "/versions", List.class);

			final List body = response.getBody();
			if (!CollectionUtils.isEmpty(body)) {
				// Assume only a single version is registered for this ID
				version = (Integer) ((Map<String, Object>) body.get(0)).get("version");
			}
		}
		catch (HttpStatusCodeException httpException) {
			throw new RuntimeException(String.format("Failed to register subject %s, server replied with status %d",
					subject, httpException.getStatusCode().value()), httpException);
		}

		SchemaRegistrationResponse schemaRegistrationResponse = new SchemaRegistrationResponse();
		schemaRegistrationResponse.setId(id);
		schemaRegistrationResponse.setSchemaReference(new SchemaReference(subject, version, "avro"));
		return schemaRegistrationResponse;
	}

	@Override
	public String fetch(SchemaReference schemaReference) {
		String path = String.format("/subjects/%s/versions/%d", schemaReference.getSubject(), schemaReference.getVersion());
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept", ACCEPT_HEADERS);
		headers.add("Content-Type", "application/vnd.schemaregistry.v1+json");
		HttpEntity<String> request = new HttpEntity<>("", headers);
		try {
			ResponseEntity<Map> response = this.template.exchange(this.endpoint + path,
					HttpMethod.GET, request, Map.class);
			return (String) response.getBody().get("schema");
		}
		catch (HttpStatusCodeException e) {
			if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
				throw new SchemaNotFoundException(String.format("Could not find schema for reference: %s", schemaReference));
			}
			else {
				throw e;
			}
		}
	}

	@Override
	public String fetch(int id) {
		String path = String.format("/schemas/ids/%d", id);
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept", ACCEPT_HEADERS);
		headers.add("Content-Type", "application/vnd.schemaregistry.v1+json");
		HttpEntity<String> request = new HttpEntity<>("", headers);
		try {
			ResponseEntity<Map> response = this.template.exchange(this.endpoint + path,
					HttpMethod.GET, request, Map.class);
			return (String) response.getBody().get("schema");
		}
		catch (HttpStatusCodeException e) {
			if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
				throw new SchemaNotFoundException(String.format("Could not find schema with id: %s", id));
			}
			else {
				throw e;
			}
		}
	}
}
