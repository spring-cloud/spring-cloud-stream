/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cloud.stream.schema.registry.SchemaReference;
import org.springframework.cloud.stream.schema.registry.SchemaRegistrationResponse;
import org.springframework.util.Assert;

/**
 * @author Vinicius Carvalho
 */
public class CachingRegistryClient implements SchemaRegistryClient {

	private static final String CACHE_PREFIX = "org.springframework.cloud.stream.schema.registry.client";

	private static final String ID_CACHE = CACHE_PREFIX + ".schemaByIdCache";

	private static final String REF_CACHE = CACHE_PREFIX + ".schemaByReferenceCache";

	private SchemaRegistryClient delegate;

	@Autowired
	private CacheManager cacheManager;

	public CachingRegistryClient(SchemaRegistryClient delegate) {
		Assert.notNull(delegate, "The delegate cannot be null");
		this.delegate = delegate;
	}

	@Override
	public SchemaRegistrationResponse register(String subject, String format, String schema) {
		SchemaRegistrationResponse response = this.delegate.register(subject, format, schema);
		this.cacheManager.getCache(ID_CACHE).put(response.getId(), schema);
		this.cacheManager.getCache(REF_CACHE).put(response.getSchemaReference(), schema);
		return response;
	}

	@Override
	@Cacheable(cacheNames = REF_CACHE)
	public String fetch(SchemaReference schemaReference) {
		return this.delegate.fetch(schemaReference);
	}

	@Override
	@Cacheable(cacheNames = ID_CACHE)
	public String fetch(int id) {
		return this.delegate.fetch(id);
	}

}
