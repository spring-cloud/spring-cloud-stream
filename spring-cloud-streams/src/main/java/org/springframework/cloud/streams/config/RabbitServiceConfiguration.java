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

package org.springframework.cloud.streams.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.xd.dirt.integration.rabbit.RabbitMessageBus;

/**
 * Bind to services, either locally or in a Lattice environment.
 *
 * @author Mark Fisher
 * @author Dave Syer
 */
@Configuration
@ConditionalOnClass(RabbitMessageBus.class)
@ConditionalOnMissingBean(RabbitMessageBus.class)
@ImportResource({ "classpath*:/META-INF/spring-xd/bus/rabbit-bus.xml",
"classpath*:/META-INF/spring-xd/analytics/rabbit-analytics.xml" })
@PropertySource("classpath:/META-INF/spring-cloud-streams/rabbit-bus.properties")
public class RabbitServiceConfiguration {
	@Configuration
	@Profile("cloud")
	protected static class CloudConfig {
		@Bean
		public Cloud cloud() {
			return new CloudFactory().getCloud();
		}
		@Bean
		ConnectionFactory redisConnectionFactory(Cloud cloud) {
			return cloud.getSingletonServiceConnector(ConnectionFactory.class, null);
		}
	}
}
