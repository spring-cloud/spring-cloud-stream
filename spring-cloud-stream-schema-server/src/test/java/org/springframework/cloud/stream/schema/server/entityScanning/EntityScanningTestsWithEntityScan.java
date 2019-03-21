/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.schema.server.entityScanning;

import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cloud.stream.schema.server.EnableSchemaRegistryServer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Marius Bogoevici
 */
public class EntityScanningTestsWithEntityScan {

	@Test
	public void testApplicationWithEmbeddedSchemaRegistryServerOutsideOfRootPackage()
			throws Exception {
		final ConfigurableApplicationContext context = SpringApplication
				.run(CustomApplicationEmbeddingSchemaServer.class, "--server.port=0");
		context.close();
	}

	@EnableAutoConfiguration
	@EnableSchemaRegistryServer
	@EntityScan(basePackages = "org.springframework.cloud.stream.schema.server.entityScanning.domain")
	public static class CustomApplicationEmbeddingSchemaServer {

	}

}
