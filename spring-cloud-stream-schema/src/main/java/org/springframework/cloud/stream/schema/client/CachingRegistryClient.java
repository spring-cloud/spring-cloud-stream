package org.springframework.cloud.stream.schema.client;

import org.springframework.cloud.stream.schema.SchemaReference;
import org.springframework.cloud.stream.schema.SchemaRegistrationResponse;

/**
 * @author Vinicius Carvalho
 */
public class CachingRegistryClient implements SchemaRegistryClient {

	private SchemaRegistryClient delegate;

	public CachingRegistryClient(SchemaRegistryClient delegate) {
		this.delegate = delegate;
	}

	@Override
	public SchemaRegistrationResponse register(String subject, String format, String schema) {
		return null;
	}

	@Override
	public String fetch(SchemaReference schemaReference) {
		return null;
	}

	@Override
	public String fetch(int id) {
		return null;
	}

}
