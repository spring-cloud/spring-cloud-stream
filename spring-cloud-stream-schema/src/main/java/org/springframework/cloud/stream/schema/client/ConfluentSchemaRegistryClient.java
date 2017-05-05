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

package org.springframework.cloud.stream.schema.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

/**
 * @author Vinicius Carvalho
 * @author Marius Bogoevici
 * @author Jon Archer
 */
public class ConfluentSchemaRegistryClient implements SchemaRegistryClient {

	private RestTemplate template;

	private String endpoint = "http://localhost:8081";

	private ObjectMapper mapper;

	public ConfluentSchemaRegistryClient() {
		this.template = new RestTemplate();
		this.mapper = new ObjectMapper();
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	@Override
	public SchemaRegistrationResponse register(String subject, String format, String schema) {
		Assert.isTrue("avro".equals(format), "Only Avro is supported");
		String path = String.format("/subjects/%s", subject);
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept",
				Arrays.asList("application/vnd.schemaregistry.v1+json", "application/vnd.schemaregistry+json",
						"application/json"));
		headers.add("Content-Type", "application/json");
		Integer version = null;
		try {
			String payload = this.mapper.writeValueAsString(Collections.singletonMap("schema", schema));
			HttpEntity<String> request = new HttpEntity<>(payload, headers);
			ResponseEntity<Map> response = this.template.exchange(this.endpoint + path, HttpMethod.POST, request,
					Map.class);
			version = (Integer) response.getBody().get("version");
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		SchemaRegistrationResponse schemaRegistrationResponse = new SchemaRegistrationResponse();
		schemaRegistrationResponse.setId(version);
		schemaRegistrationResponse.setSchemaReference(new SchemaReference(subject, version, "avro"));
		return schemaRegistrationResponse;
	}

	@Override
	public String fetch(SchemaReference schemaReference) {
		String path = String.format("/subjects/%s/versions/%d",
				schemaReference.getSubject(), schemaReference.getVersion());
		HttpHeaders headers = new HttpHeaders();
		headers.put("Accept",
				Arrays.asList("application/vnd.schemaregistry.v1+json", "application/vnd.schemaregistry+json",
						"application/json"));
		headers.add("Content-Type", "application/vnd.schemaregistry.v1+json");
		HttpEntity<String> request = new HttpEntity<>("", headers);
		try {
			ResponseEntity<Map> response = this.template.exchange(this.endpoint + path, HttpMethod.GET, request,
					Map.class);
			return (String) response.getBody().get("schema");
		}
		catch (HttpClientErrorException e) {
			if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
				throw new SchemaNotFoundException(
						String.format("Could not find schema for reference: %s", schemaReference));
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
		headers.put("Accept",
				Arrays.asList("application/vnd.schemaregistry.v1+json", "application/vnd.schemaregistry+json",
						"application/json"));
		headers.add("Content-Type", "application/vnd.schemaregistry.v1+json");
		HttpEntity<String> request = new HttpEntity<>("", headers);
		try {
			ResponseEntity<Map> response = this.template.exchange(this.endpoint + path, HttpMethod.GET, request,
					Map.class);
			return (String) response.getBody().get("schema");
		}
		catch (HttpClientErrorException e) {
			if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
				throw new SchemaNotFoundException(
						String.format("Could not find schema with id: %s", id));
			}
			else {
				throw e;
			}
		}
	}
}
