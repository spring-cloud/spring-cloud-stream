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

package org.springframework.cloud.stream.schema.server.controllers;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.cloud.stream.schema.server.config.SchemaServerProperties;
import org.springframework.cloud.stream.schema.server.model.Schema;
import org.springframework.cloud.stream.schema.server.repository.SchemaRepository;
import org.springframework.cloud.stream.schema.server.support.InvalidSchemaException;
import org.springframework.cloud.stream.schema.server.support.SchemaDeletionNotAllowedException;
import org.springframework.cloud.stream.schema.server.support.SchemaNotFoundException;
import org.springframework.cloud.stream.schema.server.support.SchemaValidator;
import org.springframework.cloud.stream.schema.server.support.UnsupportedFormatException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @author Vinicius Carvalho
 * @author Ilayaperumal Gopinathan
 */
@RestController
@RequestMapping(path = "${spring.cloud.stream.schema.server.path:}")
public class ServerController {

	private final SchemaRepository repository;

	private final Map<String, SchemaValidator> validators;

	private final SchemaServerProperties schemaServerProperties;

	public ServerController(SchemaRepository repository,
			Map<String, SchemaValidator> validators,
			SchemaServerProperties schemaServerProperties) {
		Assert.notNull(repository, "cannot be null");
		Assert.notEmpty(validators, "cannot be empty");
		this.repository = repository;
		this.validators = validators;
		this.schemaServerProperties = schemaServerProperties;
	}

	@RequestMapping(method = RequestMethod.POST, path = "/", consumes = "application/json", produces = "application/json")
	public synchronized ResponseEntity<Schema> register(@RequestBody Schema schema,
			UriComponentsBuilder builder) {
		SchemaValidator validator = this.validators.get(schema.getFormat());

		if (validator == null) {
			throw new UnsupportedFormatException(
					String.format("Invalid format, supported types are: %s", StringUtils
							.collectionToCommaDelimitedString(this.validators.keySet())));
		}

		if (!validator.isValid(schema.getDefinition())) {
			throw new InvalidSchemaException("Invalid schema");
		}

		Schema result;
		List<Schema> registeredEntities = this.repository
				.findBySubjectAndFormatOrderByVersion(schema.getSubject(),
						schema.getFormat());
		if (registeredEntities == null || registeredEntities.size() == 0) {
			schema.setVersion(1);
			result = this.repository.save(schema);
		}
		else {
			result = validator.match(registeredEntities, schema.getDefinition());
			if (result == null) {
				schema.setVersion(
						registeredEntities.get(registeredEntities.size() - 1).getVersion()
								+ 1);
				result = this.repository.save(schema);
			}

		}

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.LOCATION,
				builder.path("/{subject}/{format}/v{version}")
						.buildAndExpand(result.getSubject(), result.getFormat(),
								result.getVersion())
						.toString());
		ResponseEntity<Schema> response = new ResponseEntity<>(result, headers,
				HttpStatus.CREATED);

		return response;

	}

	@RequestMapping(method = RequestMethod.GET, produces = "application/json", path = "/{subject}/{format}/v{version}")
	public ResponseEntity<Schema> findOne(@PathVariable("subject") String subject,
			@PathVariable("format") String format,
			@PathVariable("version") Integer version) {
		Schema schema = this.repository.findOneBySubjectAndFormatAndVersion(subject,
				format, version);
		if (schema == null) {
			throw new SchemaNotFoundException("Could not find Schema");
		}
		return new ResponseEntity<>(schema, HttpStatus.OK);
	}

	@RequestMapping(method = RequestMethod.GET, produces = "application/json", path = "/schemas/{id}")
	public ResponseEntity<Schema> findOne(@PathVariable("id") Integer id) {
		Optional<Schema> schema = this.repository.findById(id);
		if (!schema.isPresent()) {
			throw new SchemaNotFoundException("Could not find Schema");
		}
		return new ResponseEntity<>(schema.get(), HttpStatus.OK);
	}

	@RequestMapping(method = RequestMethod.GET, produces = "application/json", path = "/{subject}/{format}")
	public ResponseEntity<List<Schema>> findBySubjectAndVersion(
			@PathVariable("subject") String subject,
			@PathVariable("format") String format) {
		List<Schema> schemas = this.repository
				.findBySubjectAndFormatOrderByVersion(subject, format);
		if (schemas == null || schemas.size() == 0) {
			throw new SchemaNotFoundException(String.format(
					"No schemas found for subject %s and format %s", subject, format));
		}
		return new ResponseEntity<List<Schema>>(schemas, HttpStatus.OK);
	}

	@RequestMapping(value = "/{subject}/{format}/v{version}", method = RequestMethod.DELETE)
	public void delete(@PathVariable("subject") String subject,
			@PathVariable("format") String format,
			@PathVariable("version") Integer version) {
		if (this.schemaServerProperties.isAllowSchemaDeletion()) {
			Schema schema = this.repository.findOneBySubjectAndFormatAndVersion(subject,
					format, version);
			deleteSchema(schema);
		}
		else {
			throw new SchemaDeletionNotAllowedException();
		}
	}

	@RequestMapping(value = "/schemas/{id}", method = RequestMethod.DELETE)
	public void delete(@PathVariable("id") Integer id) {
		if (this.schemaServerProperties.isAllowSchemaDeletion()) {
			Optional<Schema> schema = this.repository.findById(id);
			if (!schema.isPresent()) {
				throw new SchemaNotFoundException("Could not find Schema");
			}
			deleteSchema(schema.get());
		}
		else {
			throw new SchemaDeletionNotAllowedException();
		}
	}

	@RequestMapping(value = "/{subject}", method = RequestMethod.DELETE)
	public void delete(@PathVariable("subject") String subject) {
		if (this.schemaServerProperties.isAllowSchemaDeletion()) {
			for (Schema schema : this.repository.findAll()) {
				if (schema.getSubject().equals(subject)) {
					deleteSchema(schema);
				}
			}
		}
		else {
			throw new SchemaDeletionNotAllowedException();
		}

	}

	private void deleteSchema(Schema schema) {
		if (schema == null) {
			throw new SchemaNotFoundException("Could not find Schema");
		}
		this.repository.delete(schema);
	}

	@ExceptionHandler(UnsupportedFormatException.class)
	@ResponseStatus(value = HttpStatus.BAD_REQUEST, reason = "Format not supported")
	public void unsupportedFormat(UnsupportedFormatException ex) {
	}

	@ExceptionHandler(InvalidSchemaException.class)
	@ResponseStatus(value = HttpStatus.BAD_REQUEST, reason = "Invalid schema")
	public void invalidSchema(InvalidSchemaException ex) {
	}

	@ExceptionHandler(SchemaNotFoundException.class)
	@ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Schema not found")
	public void schemaNotFound(SchemaNotFoundException ex) {
	}

	@ExceptionHandler(SchemaDeletionNotAllowedException.class)
	@ResponseStatus(value = HttpStatus.METHOD_NOT_ALLOWED, reason = "Schema deletion is not permitted")
	public void schemaDeletionNotPermitted(SchemaDeletionNotAllowedException ex) {
	}

}
