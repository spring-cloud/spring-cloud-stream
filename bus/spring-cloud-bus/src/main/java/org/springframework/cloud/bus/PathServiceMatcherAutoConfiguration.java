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

/**
 * @author Ryan Baxter
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBusEnabled
@EnableConfigurationProperties(BusProperties.class)
public class PathServiceMatcherAutoConfiguration {

	/**
	 * Name of the Bus path matcher.
	 */
	public static final String BUS_PATH_MATCHER_NAME = "busPathMatcher";

	/**
	 * Name of the Spring Cloud Config property.
	 */
	public static final String CLOUD_CONFIG_NAME_PROPERTY = "spring.cloud.config.name";

	@BusPathMatcher
	// There is a @Bean of type PathMatcher coming from Spring MVC
	@ConditionalOnMissingBean(name = BUS_PATH_MATCHER_NAME)
	@Bean(name = BUS_PATH_MATCHER_NAME)
	public PathMatcher busPathMatcher() {
		return new DefaultBusPathMatcher(new AntPathMatcher(":"));
	}

	@Bean
	@ConditionalOnMissingBean(ServiceMatcher.class)
	public PathServiceMatcher pathServiceMatcher(@BusPathMatcher PathMatcher pathMatcher, BusProperties properties,
			Environment environment) {
		String[] configNames = environment.getProperty(CLOUD_CONFIG_NAME_PROPERTY, String[].class, new String[] {});
		return new PathServiceMatcher(pathMatcher, properties.getId(), configNames);
	}

}
