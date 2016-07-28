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

package org.springframework.cloud.stream.schema.server;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.cloud.stream.schema.server.model.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Vinicius Carvalho
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class SchemaRegistryServerAvroTests {

	final String USER_SCHEMA_V1 = "{\"namespace\": \"example.avro\",\n"
			+ " \"type\": \"record\",\n" + " \"name\": \"User\",\n" + " \"fields\": [\n"
			+ "     {\"name\": \"name\", \"type\": \"string\"},\n"
			+ "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}\n"
			+ " ]\n" + "}";

	final String USER_SCHEMA_V2 = "{\"namespace\": \"example.avro\",\n"
			+ " \"type\": \"record\",\n" + " \"name\": \"User\",\n" + " \"fields\": [\n"
			+ "     {\"name\": \"name\", \"type\": \"string\"},\n"
			+ "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n"
			+ "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
			+ " ]\n" + "}";

	@Autowired
	private TestRestTemplate client;

	@Test
	public void testUnsupportedFormat() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("spring");
		schema.setSubject("boot");
		ResponseEntity<Schema> response = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
	}

	@Test
	public void testInvalidSchema() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("boot");
		schema.setDefinition("{}");
		ResponseEntity<Schema> response = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
	}

	@Test
	public void testUserSchemaV1() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("org.springframework.cloud.stream.schema.User");
		schema.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertTrue(response.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(new Integer(1), response.getBody().getVersion());
		List<String> location = response.getHeaders().get(HttpHeaders.LOCATION);
		Assert.assertNotNull(location);
		ResponseEntity<Schema> persistedSchema = client.getForEntity(location.get(0),
				Schema.class);
		Assert.assertEquals(response.getBody().getId(),
				persistedSchema.getBody().getId());

	}

	@Test
	public void testUserSchemaV2() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("org.springframework.cloud.stream.schema.User");
		schema.setDefinition(USER_SCHEMA_V1);

		Schema schema2 = new Schema();
		schema2.setFormat("avro");
		schema2.setSubject("org.springframework.cloud.stream.schema.User");
		schema2.setDefinition(USER_SCHEMA_V2);

		ResponseEntity<Schema> response = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertTrue(response.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(new Integer(1), response.getBody().getVersion());
		List<String> location = response.getHeaders().get(HttpHeaders.LOCATION);
		Assert.assertNotNull(location);

		ResponseEntity<Schema> response2 = client.postForEntity("http://localhost:8990/",
				schema2, Schema.class);
		Assert.assertTrue(response.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(new Integer(2), response2.getBody().getVersion());
		List<String> location2 = response2.getHeaders().get(HttpHeaders.LOCATION);
		Assert.assertNotNull(location2);

	}

	@Test
	public void testIdempotentRegistration() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("org.springframework.cloud.stream.schema.User");
		schema.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertTrue(response.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(new Integer(1), response.getBody().getVersion());
		List<String> location = response.getHeaders().get(HttpHeaders.LOCATION);
		Assert.assertNotNull(location);
		ResponseEntity<Schema> response2 = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertEquals(response.getBody().getId(), response2.getBody().getId());

	}

	@Test
	public void testSchemaNotfound() throws Exception {
		ResponseEntity<Schema> response = client
				.getForEntity("http://localhost:8990/foo/avro/v42", Schema.class);
		Assert.assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
	}

	@TestConfiguration
	static class Config {
		@Bean
		public TestRestTemplate testRestTemplate() {
			return new TestRestTemplate();
		}
	}

}
