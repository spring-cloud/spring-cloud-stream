/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.test.ImportAutoConfiguration;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

/**
 * Bind to Kafka services.
 *
 * @author Marius Bogoevici
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@ImportAutoConfiguration(KafkaMessageChannelBinderConfiguration.class)
public class KafkaServiceAutoConfiguration {

	@Configuration
	@Profile("!lattice")
	@PropertySource("classpath:/META-INF/spring-cloud-stream/kafka-binder.properties")
	public static class DefaultProperties {

	}

	@Configuration
	@Profile("lattice")
	@PropertySource("classpath:/META-INF/spring-cloud-stream/kafka-binder-lattice.properties")
	public static class LatticeDefaultProperties {
	}
}
