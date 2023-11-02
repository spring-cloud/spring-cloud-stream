/*
 * Copyright 2016-2023 the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.server;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.avro.Schema.Parser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.cloud.stream.schema.registry.config.SchemaServerProperties;
import org.springframework.cloud.stream.schema.registry.controllers.ServerController;
import org.springframework.cloud.stream.schema.registry.model.Schema;
import org.springframework.cloud.stream.schema.registry.support.SchemaNotFoundException;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponentsBuilder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

/**
 * @author Vinicius Carvalho
 * @author Ilayaperumal Gopinathan
 * @author Christian Tzolov
 * @author Soby Chacko
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
class SchemaRegistryServerAvroTests {

	private static final String AVRO_FORMAT_NAME = "avro";

	private static final org.apache.avro.Schema AVRO_USER_AVRO_SCHEMA_V1 = new Parser()
			.parse(resourceToString("classpath:/avro_user_definition_schema_v1.json"));

	private static final org.apache.avro.Schema AVRO_USER_AVRO_SCHEMA_V2 = new Parser()
			.parse(resourceToString("classpath:/avro_user_definition_schema_v2.json"));

	private static final String AVRO_USER_SCHEMA_DEFAULT_NAME_STRATEGY_SUBJECT = AVRO_USER_AVRO_SCHEMA_V1.getName()
			.toLowerCase();


	private static final String AVRO_USER_SCHEMA_QUALIFED_NAME_STRATEGY_SUBJECT = AVRO_USER_AVRO_SCHEMA_V1
			.getFullName()
			.toLowerCase();

	private static final Schema AVRO_USER_REGISTRY_SCHEMA_V1 = toSchema(
			AVRO_USER_SCHEMA_DEFAULT_NAME_STRATEGY_SUBJECT,
			AVRO_FORMAT_NAME, AVRO_USER_AVRO_SCHEMA_V1.toString());

	private static final Schema AVRO_USER_REGISTRY_SCHEMA_V2 = toSchema(
			AVRO_USER_SCHEMA_DEFAULT_NAME_STRATEGY_SUBJECT,
			AVRO_FORMAT_NAME, AVRO_USER_AVRO_SCHEMA_V2.toString());

	private static final Schema AAVRO_USER_REGISTRY_SCHEMA_V1_WITH_QUAL_SUBJECT = toSchema(
			AVRO_USER_SCHEMA_QUALIFED_NAME_STRATEGY_SUBJECT,
			AVRO_FORMAT_NAME, AVRO_USER_AVRO_SCHEMA_V1.toString());
	@Autowired
	private TestRestTemplate client;

	@Autowired
	private SchemaServerProperties schemaServerProperties;

	@Autowired
	private ServerController serverController;

	@Autowired
	private ServerProperties serverProperties;

	private URI serverControllerUri;

	@BeforeEach
	public void setUp() {

		String scheme = Optional.ofNullable(this.serverProperties.getSsl())
				.filter(ssl -> ssl.isEnabled())
				.map(ssl -> "https").orElse("http");

		Integer port = this.serverProperties.getPort();
		String contextPath = this.serverProperties.getServlet().getContextPath();

		this.serverControllerUri = UriComponentsBuilder.newInstance().scheme(scheme)
				.host("localhost")
				.port(port)
				.path(contextPath).build().toUri();

		this.client.getRestTemplate().setErrorHandler(new DefaultResponseErrorHandler());

	}

	@NonNull
	static Schema toSchema(String subject, String format, String definition) {
		Schema schema = new Schema();
		schema.setSubject(subject);
		schema.setFormat(format);
		schema.setDefinition(definition);
		return schema;
	}

	@Test
	public void testUnsupportedFormat() {
		Schema schema = toSchema("spring", "boot", null);
		try {
			this.client.postForEntity(this.serverControllerUri, schema, Schema.class);
			fail("Expects: " + HttpStatus.BAD_REQUEST + " error");
		}
		catch (HttpClientErrorException.BadRequest badRequest) {
			assertThat(badRequest.getMessage()).isEqualTo("400 : \"Format not supported: Invalid format, supported types are: avro\"");
		}

	}

	@Test
	public void testInvalidSchema() {
		Schema schema = toSchema("boot", AVRO_FORMAT_NAME, "{}");
		try {
			this.client.postForEntity(this.serverControllerUri, schema, Schema.class);
			fail("Expects: " + HttpStatus.BAD_REQUEST + " error");
		}
		catch (HttpClientErrorException.BadRequest badRequest) {
			assertThat(badRequest.getMessage()).isEqualTo("400 : \"Invalid Schema: No type: {}\"");
		}

	}

	@Test
	public void testInvalidSchemaGh22() {
		Schema schema = toSchema("boot", AVRO_FORMAT_NAME,
				resourceToString("classpath:/invalid_schema.json"));
		try {
			this.client.postForEntity(this.serverControllerUri, schema, Schema.class);
			fail("Expects: " + HttpStatus.BAD_REQUEST + " error");
		}
		catch (HttpClientErrorException.BadRequest badRequest) {
			assertThat(badRequest.getMessage()).isEqualTo("400 : \"Invalid Schema: \"SomeType\" is not a defined name. " +
					"The type of the \"field\" field must be a defined name or a {\"type\": ...} expression.\"");
		}

	}

	@Test
	public void testRegister1AvroSchema() {
		Schema schema = toSchema("org.springframework.cloud.stream.schema.User", AVRO_FORMAT_NAME,
				resourceToString("classpath:/avro_user_definition_schema_v1.json"));
		registerSchemaAndAssertSuccess(schema, 1, 1);

	}

	@Test
	public void testFindByIdFound() {

		ResponseEntity<Schema> registerSchemaReponse = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		Schema registeredSchema = registerSchemaReponse.getBody();

		URI findByIdUriId1 = this.serverControllerUri.resolve("/schemas/" + registeredSchema.getId());

		ResponseEntity<Schema> findByIdResponse = this.client
				.getForEntity(findByIdUriId1, Schema.class);

		assertThat(findByIdResponse.getStatusCode().is2xxSuccessful()).isTrue();

		Schema actual = findByIdResponse.getBody();
		assertSchema(registeredSchema, actual);
	}

	@Test
	public void testFindByIdNotFound() {

		registerSchemaAndAssertSuccess(AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		URI findByIdUriId1 = this.serverControllerUri.resolve("/schemas/" + 2);

		try {
			this.client.getForEntity(findByIdUriId1, Schema.class);
			fail("Expects: " + HttpStatus.NOT_FOUND + " error");
		}
		catch (HttpClientErrorException.NotFound notFound) {
			assertThat(notFound.getMessage()).isEqualTo("404 : \"Schema not found: Could not find Schema by id: 2\"");
		}
	}

	@Test
	public void testUserSchemaV2() {
		registerSchemasAndAssertSuccess(AVRO_USER_REGISTRY_SCHEMA_V1, AVRO_USER_REGISTRY_SCHEMA_V2);
	}

	@Test
	public void testIdempotentRegistration() {

		registerSchemaAndAssertSuccess(AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		registerSchemaAndAssertSuccess(AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

	}

	//@Test(expected = HttpClientErrorException.NotFound.class)
	public void testSchemaNotfound() {
		this.client.getForEntity("http://localhost:8990/foo/avro/v42", Schema.class);
	}

	@Test
	public void testSchemaDeletionBySubjectFormatVersion() {

		ResponseEntity<Schema> registerSchemaAndAssertSuccess = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		this.schemaServerProperties.setAllowSchemaDeletion(true);

		URI subjectFormatVersionUri = this.serverControllerUri
				.resolve(registerSchemaAndAssertSuccess.getHeaders().getLocation());


		ResponseEntity<Void> deleteResponse = this.client.exchange(
				new RequestEntity<>(HttpMethod.DELETE, subjectFormatVersionUri),
				Void.class);

		assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

		try {
			this.client.getForEntity(subjectFormatVersionUri, Schema.class);
		}
		catch (HttpClientErrorException.NotFound notFound) {
			assertThat(notFound.getMessage()).isEqualTo("404 : \"Schema not found: Could not find Schema by " +
					"subject: user, format: avro, version 1\"");
		}

	}

	@Test
	public void testSchemaDeletionBySubjectFormatVersionNotFound() {

		ResponseEntity<Schema> registerSchemaAndAssertSuccess = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		this.schemaServerProperties.setAllowSchemaDeletion(true);

		URI subjectFormatVersionUri = this.serverControllerUri
				.resolve(registerSchemaAndAssertSuccess.getHeaders()
						.getLocation().toString().replace("v1", "v100"));

		try {
			this.client.exchange(new RequestEntity<>(HttpMethod.DELETE, subjectFormatVersionUri), Void.class);
		}
		catch (HttpClientErrorException.NotFound notFound) {
			assertThat(notFound.getMessage()).isEqualTo("404 : \"Schema not found: Could not find Schema by " +
					"subject: user, format: avro, version 100\"");
		}

	}

	@Test
	public void testSchemaDeletionBySubjectFormatVersionNotAllowed() {

		ResponseEntity<Schema> registerSchemaAndAssertSuccess = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		URI versionUri = this.serverControllerUri.resolve(registerSchemaAndAssertSuccess.getHeaders().getLocation());

		try {
			this.client.exchange(new RequestEntity<>(HttpMethod.DELETE, versionUri), Void.class);
		}
		catch (HttpClientErrorException.MethodNotAllowed methodNotAllowed) {
			assertThat(methodNotAllowed.getMessage()).isEqualTo("405 : \"Schema deletion is not permitted: Not permitted " +
					"deletion of Schema by subject: user, format: avro, version 1\"");
		}

	}

	@Test
	public void testSchemaDeletionById() {

		ResponseEntity<Schema> registerSchemaAndAssertSuccess = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		this.schemaServerProperties.setAllowSchemaDeletion(true);
		this.client.delete(this.serverControllerUri
				.resolve("/schemas/" + registerSchemaAndAssertSuccess.getBody().getVersion()));

		try {
			this.client.getForEntity(registerSchemaAndAssertSuccess.getHeaders().getLocation(), Schema.class);
			fail("Expects: " + HttpStatus.NOT_FOUND + " error");
		}
		catch (HttpClientErrorException.NotFound notFound) {
			assertThat(notFound.getMessage()).isEqualTo("404 : \"Schema not found: Could not find Schema by subject: " +
					"user, format: avro, version 1\"");
		}

	}

	@Test
	public void testSchemaDeletionByIdNotFound() {

		registerSchemaAndAssertSuccess(AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		this.schemaServerProperties.setAllowSchemaDeletion(true);

		try {
			this.client.exchange(new RequestEntity<>(HttpMethod.DELETE,
					this.serverControllerUri.resolve("/schemas/" + 2)), Void.class);
			fail("Expects: " + HttpStatus.NOT_FOUND + " error");
		}
		catch (HttpClientErrorException.NotFound notFound) {
			assertThat(notFound.getMessage()).isEqualTo("404 : \"Schema not found: Could not find Schema by id: 2\"");
		}

	}

	@Test
	public void testSchemaDeletionByIdNotAllowed() {

		ResponseEntity<Schema> registerSchemaAndAssertSuccess = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		URI schemaIdUri = this.serverControllerUri.resolve(this.serverControllerUri
				.resolve("/schemas/" + registerSchemaAndAssertSuccess.getBody().getVersion()));

		try {
			this.client.exchange(new RequestEntity<>(HttpMethod.DELETE, schemaIdUri), Void.class);
			fail("Expects: " + HttpStatus.METHOD_NOT_ALLOWED + " error");
		}
		catch (HttpClientErrorException.MethodNotAllowed methodNotAllowed) {
			assertThat(methodNotAllowed.getMessage()).isEqualTo("405 : \"Schema deletion is not permitted: Not " +
					"permitted deletion of Schema by id: 1\"");
		}

	}

	@Test
	public void testSchemaDeletionBySubject() {
		Map<String, Map<String, List<ResponseEntity<Schema>>>> registerSchemaResponsesByFormatBySubject = registerSchemasAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1,
				AVRO_USER_REGISTRY_SCHEMA_V2, AAVRO_USER_REGISTRY_SCHEMA_V1_WITH_QUAL_SUBJECT);

		this.schemaServerProperties.setAllowSchemaDeletion(true);

		registerSchemaResponsesByFormatBySubject.forEach((subject, registerSchemaResponsesByFormat) -> {

			assertThat(registerSchemaResponsesByFormat).isNotEmpty();
			ResponseEntity<Void> deleteBySubject = this.client.exchange(
					new RequestEntity<>(HttpMethod.DELETE, this.serverControllerUri
							.resolve("/" + subject)),
					Void.class);

			assertThat(deleteBySubject.getStatusCode()).isEqualTo(HttpStatus.OK);

			registerSchemaResponsesByFormat.forEach((format, registerSchemaResponses) -> {

				assertThat(registerSchemaResponses).isNotEmpty();

				registerSchemaResponses.forEach(registerSchemaResponse -> {

					try {
						this.client.getForEntity(registerSchemaResponse.getHeaders().getLocation(), Schema.class);
						fail("Expects: " + HttpStatus.NOT_FOUND + " error");
					}
					catch (HttpClientErrorException.NotFound notFound) {
						//do nothing
					}
				});
			});
		});

	}

	@Test
	public void testSchemaDeletionBySubjectNotFound() {

		registerSchemaAndAssertSuccess(AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		this.schemaServerProperties.setAllowSchemaDeletion(true);

		ResponseEntity<Void> deleteBySubject = this.client.exchange(
				new RequestEntity<>(HttpMethod.DELETE, this.serverControllerUri.resolve("/foo")), Void.class);

		assertThat(deleteBySubject.getStatusCode()).isEqualTo(HttpStatus.OK);

	}

	@Test
	public void testSchemaDeletionBySubjectNotAllowed() {

		ResponseEntity<Schema> registerSchemaAndAssertSuccess = registerSchemaAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1, 1, 1);

		Schema schema = registerSchemaAndAssertSuccess.getBody();

		try {
			this.client.exchange(new RequestEntity<>(HttpMethod.DELETE,
					this.serverControllerUri.resolve("/" + schema.getSubject())), Void.class);
			fail("Expects: " + HttpStatus.METHOD_NOT_ALLOWED + " error");
		}
		catch (HttpClientErrorException.MethodNotAllowed methodNotAllowed) {
			assertThat(methodNotAllowed.getMessage()).isEqualTo("405 : \"Schema deletion is not permitted: " +
					"Not permitted deletion of Schema by subject: user\"");
		}

	}

	@Test
	public void testFindSchemasBySubjectAndVersion() {

		Map<String, Map<String, List<ResponseEntity<Schema>>>> registerSchemaResponsesByFormatBySubject = registerSchemasAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1,
				AVRO_USER_REGISTRY_SCHEMA_V2);

		registerSchemaResponsesByFormatBySubject.forEach((subject, schemasByFormat) -> {

			assertThat(schemasByFormat).hasSize(1);

			schemasByFormat.forEach((format, schemas) -> {
				assertThat(schemas).hasSize(2);

				final ResponseEntity<List<Schema>> findBySubjectAndVersionResponseEntity = this.serverController
						.findBySubjectAndVersion(subject, format);

				assertThat(findBySubjectAndVersionResponseEntity.getStatusCode().is2xxSuccessful()).isTrue();

				final List<Schema> schemaResponseBody = findBySubjectAndVersionResponseEntity.getBody();

				assertThat(schemaResponseBody).zipSatisfy(schemas.stream().map(ResponseEntity::getBody)
						.collect(toList()), this::assertSchema);
			});
		});

	}

	@Test
	public void testFindBySubjectAndFormatOrderByVersionAscNoMatch() {
		String subject = "test";

		String format = AVRO_FORMAT_NAME;

		assertThatExceptionOfType(SchemaNotFoundException.class).isThrownBy(() -> this.serverController
				.findBySubjectAndFormatOrderByVersionAsc(subject, format))
				.withMessage("No schemas found for subject %s and format %s", subject, format)
				.withNoCause();

	}

	@Test
	public void testFindSchemasBySubjectAndFormat() {

		Map<String, Map<String, List<ResponseEntity<Schema>>>> registerSchemaResponsesByFormatBySubject = registerSchemasAndAssertSuccess(
				AVRO_USER_REGISTRY_SCHEMA_V1,
				AVRO_USER_REGISTRY_SCHEMA_V2);

		registerSchemaResponsesByFormatBySubject.forEach((subject, schemasByFormat) -> {

			assertThat(schemasByFormat).hasSize(1);

			schemasByFormat.forEach((format, schemas) -> {
				assertThat(schemas).hasSize(2);

				ResponseEntity<List<Schema>> findBySubjectFormatResponse = this.client.exchange(
						this.serverControllerUri.resolve("/" + subject + "/" + format), HttpMethod.GET, null,
						new ParameterizedTypeReference<List<Schema>>() {
						});

				assertThat(findBySubjectFormatResponse.getStatusCode().is2xxSuccessful()).isTrue();

				final List<Schema> schemaResponseBody = findBySubjectFormatResponse.getBody();

				assertThat(schemaResponseBody).zipSatisfy(schemas.stream().map(ResponseEntity::getBody)
						.collect(toList()), this::assertSchema);
			});
		});

	}

	private Map<String, Map<String, List<ResponseEntity<Schema>>>> registerSchemasAndAssertSuccess(
			@NonNull Schema... schemas) {
		Map<String, Map<String, Integer>> versionsByFormatAndSubject = new HashMap<>();
		Map<String, Map<String, List<ResponseEntity<Schema>>>> result = new HashMap<>();
		int numOfSchemas = schemas.length;
		int id = 0;
		for (int i = 0; i < numOfSchemas; i++) {
			Schema schema = schemas[i];
			id++;
			String format = schema.getFormat();
			String subject = schema.getSubject();
			Integer version = versionsByFormatAndSubject
					.compute(subject,
							(_subject, currentValue) -> currentValue == null ? new HashMap<>() : currentValue)
					.merge(format, 1, Integer::sum);
			ResponseEntity<Schema> registerSchemaResponse = registerSchemaAndAssertSuccess(schema, version, id);
			result.compute(subject,
					(_subject, currentValue) -> currentValue == null ? new HashMap<>() : currentValue)

					.compute(format, (_format, currentValue) -> {
						List<ResponseEntity<Schema>> value = currentValue == null ? new ArrayList<>() : currentValue;
						value.add(registerSchemaResponse);
						return value;
					});
		}
		Stream<ResponseEntity<Schema>> asStream = result.entrySet().stream()
				.map(Entry::getValue)
				.map(Map::entrySet)
				.flatMap(Collection::stream)
				.map(Entry::getValue)
				.flatMap(Collection::stream);
		assertThat(asStream).hasSize(numOfSchemas);
		return result;

	}

	@NonNull
	private ResponseEntity<Schema> registerSchemaAndAssertSuccess(@NonNull Schema schema,
			@Nullable Integer expectedVersion,
			@Nullable Integer expectedId) {

		ResponseEntity<Schema> registerReponse = this.client
				.postForEntity(this.serverControllerUri, schema, Schema.class);

		HttpStatusCode statusCode = registerReponse.getStatusCode();
		assertThat(statusCode.is2xxSuccessful()).isTrue();

		Schema registeredSchema = registerReponse.getBody();
		assertSchema(schema, expectedVersion, expectedId, registeredSchema);

		HttpHeaders headers = registerReponse.getHeaders();
		assertLocation(headers, registeredSchema);

		return registerReponse;
	}

	private void assertLocation(HttpHeaders headers, Schema registeredSchema) {
		URI location = headers.getLocation();

		assertThat(location).isNotNull();
		assertPersisted(location, registeredSchema);
	}

	private void assertPersisted(URI location, Schema registeredSchema) {

		ResponseEntity<Schema> findOneResponse = this.client.getForEntity(location,
				Schema.class);

		HttpStatusCode statusCode = findOneResponse.getStatusCode();
		assertThat(statusCode.is2xxSuccessful()).isTrue();

		Schema actual = findOneResponse.getBody();
		assertSchema(registeredSchema, registeredSchema.getVersion(), registeredSchema.getId(), actual);

	}

	private void assertSchema(@NonNull Schema expected, @NonNull Schema actual) {

		assertSchema(expected, expected.getVersion(), expected.getId(), actual);
	}

	private void assertSchema(@NonNull Schema expected, Integer expectedVersion, Integer expectedId,
			@NonNull Schema actual) {

		assertThat(actual).isEqualToIgnoringGivenFields(expected, "version", "id");
		if (expectedVersion != null) {
			assertThat(actual.getVersion()).isEqualTo(expectedVersion);
		}
		if (expectedId != null) {
			assertThat(actual.getId()).isEqualTo(expectedId);
		}
	}

	private static String resourceToString(String resourceUri) {
		try {
			return StreamUtils.copyToString(new DefaultResourceLoader().getResource(resourceUri)
					.getInputStream(), StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			throw new IllegalStateException("Could not extract resource: " + resourceUri, e);
		}
	}
}
