/*
 * Copyright 2012-2020 the original author or authors.
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

package org.springframework.cloud.bus;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

import static org.springframework.cloud.bus.BusAutoConfiguration.CLOUD_CONFIG_NAME_PROPERTY;

/**
 * @author Ryan Baxter
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBusEnabled
@EnableConfigurationProperties(BusProperties.class)
public class ServiceMatcherAutoConfiguration {

	@BusPathMatcher
	// There is a @Bean of type PathMatcher coming from Spring MVC
	@ConditionalOnMissingBean(name = BusAutoConfiguration.BUS_PATH_MATCHER_NAME)
	@Bean(name = BusAutoConfiguration.BUS_PATH_MATCHER_NAME)
	public PathMatcher busPathMatcher() {
		return new DefaultBusPathMatcher(new AntPathMatcher(":"));
	}

	@Bean
	public ServiceMatcher serviceMatcher(@BusPathMatcher PathMatcher pathMatcher, BusProperties properties,
			Environment environment) {
		String[] configNames = environment.getProperty(CLOUD_CONFIG_NAME_PROPERTY, String[].class, new String[] {});
		ServiceMatcher serviceMatcher = new ServiceMatcher(pathMatcher, properties.getId(), configNames);
		return serviceMatcher;
	}

}
