/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.entityScanning;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.schema.registry.config.SchemaServerProperties;
import org.springframework.cloud.stream.schema.registry.model.Schema;
import org.springframework.cloud.stream.schema.registry.repository.SchemaRepository;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;




/**
 * @author Christian Tzolov
 */
public abstract class AbstractServerControllerTest {

	protected MockMvc mockMvc;

	@Autowired
	protected SchemaRepository schemaRepository;

	@Autowired
	protected SchemaServerProperties schemaServerProperties;

	@Autowired
	private WebApplicationContext wac;

	@BeforeEach
	public void setupMocks() {
		this.mockMvc = MockMvcBuilders.webAppContextSetup(wac)
				.defaultRequest(get("/").accept(MediaType.APPLICATION_JSON)).build();
		Schema schema = new Schema();
		schema.setSubject("test667");
		schema.setVersion(667);
		schema.setFormat("format");
		schema.setDefinition("Test Schema Definition");
		schemaRepository.save(schema);
	}
}
