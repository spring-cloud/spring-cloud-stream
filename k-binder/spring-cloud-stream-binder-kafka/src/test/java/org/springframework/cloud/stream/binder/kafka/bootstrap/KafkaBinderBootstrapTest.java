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

package org.springframework.cloud.stream.binder.kafka.bootstrap;

import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/**
 * @author Marius Bogoevici
 */
public class KafkaBinderBootstrapTest {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 10);

	@Test
	public void testKafkaBinderConfiguration() throws Exception {
		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(
				SimpleApplication.class).web(WebApplicationType.NONE).run(
						"--spring.cloud.stream.kafka.binder.brokers="
								+ embeddedKafka.getEmbeddedKafka().getBrokersAsString(),
						"--spring.cloud.stream.kafka.binder.zkNodes=" + embeddedKafka
								.getEmbeddedKafka().getZookeeperConnectionString());
		applicationContext.close();
	}

	@SpringBootApplication
	static class SimpleApplication {

	}

}
