/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.schema.client;

import java.util.HashMap;
import java.util.Map;

import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

/**
 * @author Marius Bogoevici
 * @author Vinicius Carvalho
 */
public class DefaultSchemaRegistryClient implements SchemaRegistryClient {

	private RestTemplate restTemplate;

	private String endpoint = "http://localhost:8990";

	public DefaultSchemaRegistryClient() {
		this(new RestTemplate());
	}

	public DefaultSchemaRegistryClient(RestTemplate restTemplate) {
		Assert.notNull(restTemplate, "'restTemplate' must not be null.");
		this.restTemplate = restTemplate;
	}

	protected String getEndpoint() {
		return this.endpoint;
	}

	public void setEndpoint(String endpoint) {
		Assert.hasText(endpoint, "cannot be empty");
		this.endpoint = endpoint;
	}

	protected RestTemplate getRestTemplate() {
		return this.restTemplate;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public SchemaRegistrationResponse register(String subject, String format,
			String schema) {
		Map<String, String> requestBody = new HashMap<>();
		requestBody.put("subject", subject);
		requestBody.put("format", format);
		requestBody.put("definition", schema);
		ResponseEntity<Map> responseEntity = this.restTemplate
				.postForEntity(this.endpoint, requestBody, Map.class);
		if (responseEntity.getStatusCode().is2xxSuccessful()) {
			SchemaRegistrationResponse registrationResponse = new SchemaRegistrationResponse();
			Map<String, Object> responseBody = (Map<String, Object>) responseEntity
					.getBody();
			registrationResponse.setId((Integer) responseBody.get("id"));
			registrationResponse.setSchemaReference(
					new SchemaReference(subject, (Integer) responseBody.get("version"),
							responseBody.get("format").toString()));
			return registrationResponse;
		}
		throw new RuntimeException(
				"Failed to register schema: " + responseEntity.toString());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public String fetch(SchemaReference schemaReference) {
		ResponseEntity<Map> responseEntity = this.restTemplate.getForEntity(this.endpoint
				+ "/" + schemaReference.getSubject() + "/" + schemaReference.getFormat()
				+ "/v" + schemaReference.getVersion(), Map.class);
		if (!responseEntity.getStatusCode().is2xxSuccessful()) {
			throw new RuntimeException(
					"Failed to fetch schema: " + responseEntity.toString());
		}
		return (String) responseEntity.getBody().get("definition");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public String fetch(int id) {
		ResponseEntity<Map> responseEntity = this.restTemplate
				.getForEntity(this.endpoint + "/schemas/" + id, Map.class);
		if (!responseEntity.getStatusCode().is2xxSuccessful()) {
			throw new RuntimeException(
					"Failed to fetch schema: " + responseEntity.toString());
		}
		return (String) responseEntity.getBody().get("definition");
	}

}
