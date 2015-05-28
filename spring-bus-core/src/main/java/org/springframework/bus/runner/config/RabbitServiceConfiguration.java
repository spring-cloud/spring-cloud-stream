/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.bus.runner.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.config.java.AbstractCloudConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;
import org.springframework.xd.dirt.integration.rabbit.RabbitMessageBus;

/**
 * Bind to services, either locally or in a Lattice environment.
 *
 * @author Mark Fisher
 * @author Dave Syer
 */
@Configuration
@ConditionalOnClass(RabbitMessageBus.class)
@ImportResource({ "classpath*:/META-INF/spring-xd/bus/rabbit-bus.xml",
		"classpath*:/META-INF/spring-xd/analytics/rabbit-analytics.xml" })
public class RabbitServiceConfiguration {

	@Configuration
	@Profile("cloud")
	protected static class CloudConfig extends AbstractCloudConfig {

		@Bean
		ConnectionFactory rabbitConnectionFactory() {
			return connectionFactory().rabbitConnectionFactory();
		}
	}

}
