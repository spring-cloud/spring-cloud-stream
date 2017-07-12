/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.schema.avro.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.springframework.cloud.stream.schema.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;
import org.springframework.cloud.stream.schema.client.ConfluentSchemaRegistryClient;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;


/**
 * @author Vinicius Carvalho
 */
public class ConfluentSchemaRegistryClientTests {

	private RestTemplate restTemplate;
	private MockRestServiceServer mockRestServiceServer;

	@Before
	public void setup(){
		this.restTemplate = new RestTemplate();
		this.mockRestServiceServer = MockRestServiceServer.createServer(restTemplate);
	}

	@Test
	public void registerSchema() throws Exception{
		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type","application/json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"id\":101}", MediaType.APPLICATION_JSON));

		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type","application/json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"version\":1}", MediaType.APPLICATION_JSON));

		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(this.restTemplate);
		SchemaRegistrationResponse response = client.register("user","avro","{}");
		Assert.assertEquals(1,response.getSchemaReference().getVersion());
		Assert.assertEquals(101,response.getId());
		this.mockRestServiceServer.verify();
	}

	@Test(expected = RuntimeException.class)
	public void registerWithInvalidJson() {
		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type","application/json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withBadRequest());
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(this.restTemplate);
		SchemaRegistrationResponse response = client.register("user","avro","<>");
	}

	@Test
	public void registerIncompatibleSchema() {
		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type","application/json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withStatus(HttpStatus.CONFLICT));
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(this.restTemplate);
		Exception expected = null;
		try {
			SchemaRegistrationResponse response = client.register("user","avro","{}");
		}
		catch (Exception e) {
			expected = e;
		}
		Assert.assertTrue(expected instanceof RuntimeException);
		Assert.assertTrue(expected.getCause() instanceof HttpStatusCodeException);
		this.mockRestServiceServer.verify();
	}

	@Test
	public void responseErrorFetch() {
		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user/versions"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type","application/json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"id\":101}", MediaType.APPLICATION_JSON));

		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user"))
				.andExpect(method(HttpMethod.POST))
				.andExpect(header("Content-Type","application/json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withBadRequest());
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(this.restTemplate);
		Exception expected = null;
		try {
			SchemaRegistrationResponse response = client.register("user","avro","{}");
		}
		catch (Exception e) {
			expected = e;
		}
		Assert.assertTrue(expected instanceof RuntimeException);
		Assert.assertTrue(expected.getCause() instanceof HttpStatusCodeException);
		this.mockRestServiceServer.verify();
	}

	@Test
	public void findByReference(){
		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user/versions/1"))
				.andExpect(method(HttpMethod.GET))
				.andExpect(header("Content-Type","application/vnd.schemaregistry.v1+json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withSuccess("{\"schema\":\"\"}", MediaType.APPLICATION_JSON));
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(this.restTemplate);
		SchemaReference reference = new SchemaReference("user",1,"avro");
		String schema = client.fetch(reference);
		Assert.assertEquals("",schema);
		this.mockRestServiceServer.verify();
	}

	@Test(expected = SchemaNotFoundException.class)
	public void schemaNotFound(){
		this.mockRestServiceServer.expect(requestTo("http://localhost:8081/subjects/user/versions/1"))
				.andExpect(method(HttpMethod.GET))
				.andExpect(header("Content-Type","application/vnd.schemaregistry.v1+json"))
				.andExpect(header("Accept","application/vnd.schemaregistry.v1+json"))
				.andRespond(withStatus(HttpStatus.NOT_FOUND));
		ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient(this.restTemplate);
		SchemaReference reference = new SchemaReference("user",1,"avro");
		String schema = client.fetch(reference);
	}

}
