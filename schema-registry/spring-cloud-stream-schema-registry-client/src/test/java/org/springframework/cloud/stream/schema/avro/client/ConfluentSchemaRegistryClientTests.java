/*
 * Copyright 2017-2023 the original author or authors.
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

package org.springframework.cloud.stream.schema.avro.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.schema.registry.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.registry.SchemaReference;
import org.springframework.cloud.stream.schema.registry.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.registry.client.ConfluentSchemaRegistryClient;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * @author Vinicius Carvalho
 * @author TengZhou Dong
 */
class ConfluentSchemaRegistryClientTests {

	private RestTemplate restTemplate;

	private MockRestServiceServer mockRestServiceServer;

	@BeforeEach
	public void setup() {
		this.restTemplate = new RestTemplate();
		this.mockRestServiceServer = MockRestServiceServer
				.createServer(this.restTemplate);
	}

	@Test
	public void registerSchema() throws Exception {
		this.mockRestServiceServer
				.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type", "application/json"))
				.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"id\":101,\"version\":1}", MediaType.APPLICATION_JSON));

		this.mockRestServiceServer
				.expect(requestTo("http://localhost:8081/schemas/ids/101/versions"))
				.andExpect(method(HttpMethod.GET))
				.andRespond((withSuccess("[{\"subject\":\"user\",\"version\":1}]", MediaType.APPLICATION_JSON)));

		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
				this.restTemplate);
		SchemaRegistrationResponse response = client.register("user", "avro", "{}");
		assertThat(response.getSchemaReference().getVersion()).isEqualTo(1);
		assertThat(response.getId()).isEqualTo(101);
		this.mockRestServiceServer.verify();
	}

	@Test
	public void registerWithInvalidJson() {
		assertThatThrownBy(() -> {
			this.mockRestServiceServer
					.expect(requestTo("http://localhost:8081/subjects/user/versions"))
					.andExpect(method(HttpMethod.POST))
					.andExpect(header("Content-Type", "application/json"))
					.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
					.andRespond(withBadRequest());
			ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
					this.restTemplate);
			SchemaRegistrationResponse response = client.register("user", "avro", "<>");
		}).isInstanceOf(RuntimeException.class);
	}

	@Test
	public void registerIncompatibleSchema() {
		this.mockRestServiceServer
				.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type", "application/json"))
				.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
				.andRespond(withStatus(HttpStatus.CONFLICT));
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
				this.restTemplate);
		Exception expected = null;
		try {
			SchemaRegistrationResponse response = client.register("user", "avro", "{}");
		}
		catch (Exception e) {
			expected = e;
		}
		assertThat(expected instanceof RuntimeException).isTrue();
		assertThat(expected.getCause() instanceof HttpStatusCodeException).isTrue();
		this.mockRestServiceServer.verify();
	}

	@Test
	public void findByReference() {
		this.mockRestServiceServer
				.expect(requestTo("http://localhost:8081/subjects/user/versions/1"))
				.andExpect(method(HttpMethod.GET))
				.andExpect(
						header("Content-Type", "application/vnd.schemaregistry.v1+json"))
				.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"schema\":\"\"}", MediaType.APPLICATION_JSON));
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
				this.restTemplate);
		SchemaReference reference = new SchemaReference("user", 1, "avro");
		String schema = client.fetch(reference);
		assertThat(schema).isEqualTo("");
		this.mockRestServiceServer.verify();
	}

	@Test
	public void schemaNotFound() {
		assertThatThrownBy(() -> {
					this.mockRestServiceServer
							.expect(requestTo("http://localhost:8081/subjects/user/versions/1"))
							.andExpect(method(HttpMethod.GET))
							.andExpect(
									header("Content-Type", "application/vnd.schemaregistry.v1+json"))
							.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
							.andRespond(withStatus(HttpStatus.NOT_FOUND));
				ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
						this.restTemplate);
		SchemaReference reference = new SchemaReference("user", 1, "avro");
		client.fetch(reference);
	}).isInstanceOf(SchemaNotFoundException.class);
}

	@Test
	public void responseErrorFetch() {
		this.mockRestServiceServer
				.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type", "application/json"))
				.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
				.andRespond(withBadRequest());

		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
				this.restTemplate);
		Exception expected = null;
		try {
			SchemaRegistrationResponse response = client.register("user", "avro", "{}");
		}
		catch (Exception e) {
			expected = e;
		}
		assertThat(expected != null).isTrue();
		assertThat(expected.getCause() instanceof HttpStatusCodeException).isTrue();
		this.mockRestServiceServer.verify();
	}

	@Test
	public void fetchById() {
		this.mockRestServiceServer
				.expect(requestTo("http://localhost:8081/schemas/ids/1"))
				.andExpect(method(HttpMethod.GET))
				.andExpect(
						header("Content-Type", "application/vnd.schemaregistry.v1+json"))
				.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"schema\":\"\"}", MediaType.APPLICATION_JSON));
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
				this.restTemplate);
		String schema = client.fetch(1);
		assertThat(schema).isEqualTo("");
		this.mockRestServiceServer.verify();
	}

	@Test
	public void fetchByIdSchemaNotFound() {
		assertThatThrownBy(() -> {
			this.mockRestServiceServer
					.expect(requestTo("http://localhost:8081/schemas/ids/1"))
					.andExpect(method(HttpMethod.GET))
					.andExpect(
							header("Content-Type", "application/vnd.schemaregistry.v1+json"))
					.andExpect(header("Accept", "application/vnd.schemaregistry.v1+json"))
					.andRespond(withStatus(HttpStatus.NOT_FOUND));
			ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(
					this.restTemplate);
			client.fetch(1);
		}).isInstanceOf(SchemaNotFoundException.class);
	}
}
