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

package org.springframework.cloud.stream.schema.server;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.cloud.stream.schema.server.config.SchemaServerProperties;
import org.springframework.cloud.stream.schema.server.model.Schema;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.context.WebApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

/**
 * @author Vinicius Carvalho
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
				properties = "spring.main.allow-bean-definition-overriding=true")
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
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

	@Autowired
	private SchemaServerProperties schemaServerProperties;

	@Autowired
	private WebApplicationContext wac;

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

	@Test
	public void testSchemaDeletionBySubjectFormatVersion() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("test");
		schema.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertTrue(response1.getStatusCode().is2xxSuccessful());
		schemaServerProperties.setAllowSchemaDeletion(true);
		client.delete("http://localhost:8990/test/avro/v1");
		ResponseEntity<Schema> response2 = client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class);
		Assert.assertEquals(HttpStatus.NOT_FOUND, response2.getStatusCode());
	}

	@Test
	public void testSchemaDeletionById() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("test");
		schema.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertTrue(response1.getStatusCode().is2xxSuccessful());
		ResponseEntity<Schema> response2 = client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class);
		Assert.assertEquals(HttpStatus.OK, response2.getStatusCode());
		schemaServerProperties.setAllowSchemaDeletion(true);
		client.delete("http://localhost:8990/schemas/1");
		ResponseEntity<Schema> response3 = client
				.getForEntity("http://localhost:8990/test/avro/1", Schema.class);
		Assert.assertEquals(HttpStatus.NOT_FOUND, response3.getStatusCode());
	}

	@Test
	public void testSchemaDeletionBySubject() throws Exception {
		Schema schema1 = new Schema();
		schema1.setFormat("avro");
		schema1.setSubject("test");
		schema1.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = client.postForEntity("http://localhost:8990/",
				schema1, Schema.class);
		Assert.assertTrue(response1.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(HttpStatus.OK,
				client.getForEntity("http://localhost:8990/test/avro/v1", Schema.class).getStatusCode());
		client.getForEntity("http://localhost:8990/test/avro/1", Schema.class);
		Schema schema2 = new Schema();
		schema2.setFormat("avro");
		schema2.setSubject("test");
		schema2.setDefinition(USER_SCHEMA_V2);
		ResponseEntity<Schema> response2 = client.postForEntity("http://localhost:8990/",
				schema2, Schema.class);
		Assert.assertTrue(response2.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(HttpStatus.OK,
				client.getForEntity("http://localhost:8990/test/avro/v2", Schema.class).getStatusCode());
		schemaServerProperties.setAllowSchemaDeletion(true);
		client.delete("http://localhost:8990/test");
		ResponseEntity<Schema> response4 = client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class);
		Assert.assertEquals(HttpStatus.NOT_FOUND, response4.getStatusCode());
		ResponseEntity<Schema> response5 = client
				.getForEntity("http://localhost:8990/test/avro/v2", Schema.class);
		Assert.assertEquals(HttpStatus.NOT_FOUND, response5.getStatusCode());
	}

	@Test
	public void testSchemaDeletionNotAllowed() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("test");
		schema.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = client.postForEntity("http://localhost:8990/",
				schema, Schema.class);
		Assert.assertTrue(response1.getStatusCode().is2xxSuccessful());
		ResponseEntity<Object> deleteBySubjectFormatVersion = client.exchange("http://localhost:8990/test/avro/v1",
				HttpMethod.DELETE,
				null, Object.class);
		assertThat(deleteBySubjectFormatVersion.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
		ResponseEntity<Object> deleteBySubject = client.exchange("http://localhost:8990/test", HttpMethod.DELETE,
				null, Object.class);
		assertThat(deleteBySubject.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
		ResponseEntity<Object> deleteById = client.exchange("http://localhost:8990/schemas/1", HttpMethod.DELETE,
				null, Object.class);
		assertThat(deleteById.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
	}

	@Test
	public void testFindSchemasBySubjectAndVersion() throws Exception {
		Schema v1 = new Schema();
		v1.setFormat("avro");
		v1.setSubject("test");
		v1.setDefinition(USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = client.postForEntity("http://localhost:8990/",
				v1, Schema.class);
		Assert.assertTrue(response1.getStatusCode().is2xxSuccessful());

		Schema v2 = new Schema();
		v2.setFormat("avro");
		v2.setSubject("test");
		v2.setDefinition(USER_SCHEMA_V2);

		ResponseEntity<Schema> response2 = client.postForEntity("http://localhost:8990/",
				v2, Schema.class);
		Assert.assertTrue(response2.getStatusCode().is2xxSuccessful());

		ResponseEntity<List<Schema>> schemaResponse = client.exchange("http://localhost:8990/test/avro", HttpMethod.GET,
				null, new ParameterizedTypeReference<List<Schema>>() {
				});

		Assert.assertTrue(schemaResponse.getStatusCode().is2xxSuccessful());
		Assert.assertEquals(2, schemaResponse.getBody().size());
	}

}
