/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.schema.server;

import java.util.List;

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
// @checkstyle:off
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
// @checkstyle:on
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
		ResponseEntity<Schema> response = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
	}

	@Test
	public void testInvalidSchema() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("boot");
		schema.setDefinition("{}");
		ResponseEntity<Schema> response = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
	}

	@Test
	public void testUserSchemaV1() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("org.springframework.cloud.stream.schema.User");
		schema.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(response.getBody().getVersion()).isEqualTo(new Integer(1));
		List<String> location = response.getHeaders().get(HttpHeaders.LOCATION);
		assertThat(location).isNotNull();
		ResponseEntity<Schema> persistedSchema = this.client.getForEntity(location.get(0),
				Schema.class);
		assertThat(persistedSchema.getBody().getId())
				.isEqualTo(response.getBody().getId());

	}

	@Test
	public void testUserSchemaV2() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("org.springframework.cloud.stream.schema.User");
		schema.setDefinition(this.USER_SCHEMA_V1);

		Schema schema2 = new Schema();
		schema2.setFormat("avro");
		schema2.setSubject("org.springframework.cloud.stream.schema.User");
		schema2.setDefinition(this.USER_SCHEMA_V2);

		ResponseEntity<Schema> response = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(response.getBody().getVersion()).isEqualTo(new Integer(1));
		List<String> location = response.getHeaders().get(HttpHeaders.LOCATION);
		assertThat(location).isNotNull();

		ResponseEntity<Schema> response2 = this.client
				.postForEntity("http://localhost:8990/", schema2, Schema.class);
		assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(response2.getBody().getVersion()).isEqualTo(new Integer(2));
		List<String> location2 = response2.getHeaders().get(HttpHeaders.LOCATION);
		assertThat(location2).isNotNull();

	}

	@Test
	public void testIdempotentRegistration() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("org.springframework.cloud.stream.schema.User");
		schema.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(response.getBody().getVersion()).isEqualTo(new Integer(1));
		List<String> location = response.getHeaders().get(HttpHeaders.LOCATION);
		assertThat(location).isNotNull();
		ResponseEntity<Schema> response2 = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response2.getBody().getId()).isEqualTo(response.getBody().getId());

	}

	@Test
	public void testSchemaNotfound() throws Exception {
		ResponseEntity<Schema> response = this.client
				.getForEntity("http://localhost:8990/foo/avro/v42", Schema.class);
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}

	@Test
	public void testSchemaDeletionBySubjectFormatVersion() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("test");
		schema.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response1.getStatusCode().is2xxSuccessful()).isTrue();
		this.schemaServerProperties.setAllowSchemaDeletion(true);
		this.client.delete("http://localhost:8990/test/avro/v1");
		ResponseEntity<Schema> response2 = this.client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class);
		assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}

	@Test
	public void testSchemaDeletionById() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("test");
		schema.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response1.getStatusCode().is2xxSuccessful()).isTrue();
		ResponseEntity<Schema> response2 = this.client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class);
		assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.OK);
		this.schemaServerProperties.setAllowSchemaDeletion(true);
		this.client.delete("http://localhost:8990/schemas/1");
		ResponseEntity<Schema> response3 = this.client
				.getForEntity("http://localhost:8990/test/avro/1", Schema.class);
		assertThat(response3.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}

	@Test
	public void testSchemaDeletionBySubject() throws Exception {
		Schema schema1 = new Schema();
		schema1.setFormat("avro");
		schema1.setSubject("test");
		schema1.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = this.client
				.postForEntity("http://localhost:8990/", schema1, Schema.class);
		assertThat(response1.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(this.client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class)
				.getStatusCode()).isEqualTo(HttpStatus.OK);
		this.client.getForEntity("http://localhost:8990/test/avro/1", Schema.class);
		Schema schema2 = new Schema();
		schema2.setFormat("avro");
		schema2.setSubject("test");
		schema2.setDefinition(this.USER_SCHEMA_V2);
		ResponseEntity<Schema> response2 = this.client
				.postForEntity("http://localhost:8990/", schema2, Schema.class);
		assertThat(response2.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(this.client
				.getForEntity("http://localhost:8990/test/avro/v2", Schema.class)
				.getStatusCode()).isEqualTo(HttpStatus.OK);
		this.schemaServerProperties.setAllowSchemaDeletion(true);
		this.client.delete("http://localhost:8990/test");
		ResponseEntity<Schema> response4 = this.client
				.getForEntity("http://localhost:8990/test/avro/v1", Schema.class);
		assertThat(response4.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
		ResponseEntity<Schema> response5 = this.client
				.getForEntity("http://localhost:8990/test/avro/v2", Schema.class);
		assertThat(response5.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}

	@Test
	public void testSchemaDeletionNotAllowed() throws Exception {
		Schema schema = new Schema();
		schema.setFormat("avro");
		schema.setSubject("test");
		schema.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = this.client
				.postForEntity("http://localhost:8990/", schema, Schema.class);
		assertThat(response1.getStatusCode().is2xxSuccessful()).isTrue();
		ResponseEntity<Object> deleteBySubjectFormatVersion = this.client.exchange(
				"http://localhost:8990/test/avro/v1", HttpMethod.DELETE, null,
				Object.class);
		assertThat(deleteBySubjectFormatVersion.getStatusCode())
				.isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
		ResponseEntity<Object> deleteBySubject = this.client.exchange(
				"http://localhost:8990/test", HttpMethod.DELETE, null, Object.class);
		assertThat(deleteBySubject.getStatusCode())
				.isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
		ResponseEntity<Object> deleteById = this.client.exchange(
				"http://localhost:8990/schemas/1", HttpMethod.DELETE, null, Object.class);
		assertThat(deleteById.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
	}

	@Test
	public void testFindSchemasBySubjectAndVersion() throws Exception {
		Schema v1 = new Schema();
		v1.setFormat("avro");
		v1.setSubject("test");
		v1.setDefinition(this.USER_SCHEMA_V1);
		ResponseEntity<Schema> response1 = this.client
				.postForEntity("http://localhost:8990/", v1, Schema.class);
		assertThat(response1.getStatusCode().is2xxSuccessful()).isTrue();

		Schema v2 = new Schema();
		v2.setFormat("avro");
		v2.setSubject("test");
		v2.setDefinition(this.USER_SCHEMA_V2);

		ResponseEntity<Schema> response2 = this.client
				.postForEntity("http://localhost:8990/", v2, Schema.class);
		assertThat(response2.getStatusCode().is2xxSuccessful()).isTrue();

		ResponseEntity<List<Schema>> schemaResponse = this.client.exchange(
				"http://localhost:8990/test/avro", HttpMethod.GET, null,
				new ParameterizedTypeReference<List<Schema>>() {
				});

		assertThat(schemaResponse.getStatusCode().is2xxSuccessful()).isTrue();
		assertThat(schemaResponse.getBody().size()).isEqualTo(2);
	}

}
