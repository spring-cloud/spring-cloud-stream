/*
 * Copyright 2019-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.schema.registry.config.SchemaServerConfiguration;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Christian Tzolov
 * @author Soby Chacko
 */
@SpringBootTest(classes = { SchemaServerConfiguration.class })
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.ANY)
@EnableAutoConfiguration
@TestPropertySource(properties = {
		"spring.cloud.stream.schema.server.path=/testpath",
		"spring.cloud.stream.schema.server.allowSchemaDeletion=false"
})
class ServerControllerTest extends AbstractServerControllerTest {

	@Test
	public void propertiesTest() {
		assertThat(schemaServerProperties.getPath()).isEqualTo("/testpath");
		assertThat(schemaServerProperties.isAllowSchemaDeletion()).isEqualTo(false);
	}

	@Test
	public void findSchema() throws Exception {
		mockMvc.perform(get(schemaServerProperties.getPath() + "/test667/format/v667")
				.accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(containsString("Test Schema Definition")));
	}

}
