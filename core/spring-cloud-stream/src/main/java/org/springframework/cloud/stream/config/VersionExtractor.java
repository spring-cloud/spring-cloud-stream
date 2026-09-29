/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.stream.function.FunctionConfiguration;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * @author Oleg Zhurakousky
 * @author Nicolo Pietro Belcastro
 * @since 4.2.x
 */
class VersionExtractor implements EnvironmentPostProcessor {

	protected final Log logger = LogFactory.getLog(getClass());

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		String streamVersion = this.extractVersion(FunctionConfiguration.class);
		if (logger.isDebugEnabled()) {
			logger.debug("Spring Cloud Stream version: " + streamVersion);
		}
		System.setProperty("spring-cloud-stream.version", streamVersion);
		String functionVersion = this.extractVersion(FunctionCatalog.class);
		if (logger.isDebugEnabled()) {
			logger.debug("Spring Cloud Function version: " + functionVersion);
		}
		System.setProperty("spring-cloud-function.version", functionVersion);
	}

	private String extractVersion(Class<?> clazz) {
		try {
			Package pkg = clazz.getPackage();
			String version = (pkg != null) ? pkg.getImplementationVersion() : null;
			return (version != null) ? version : "";
		}
		catch (Throwable e) {
			logger.warn("Failed to determine version of: " + clazz.getName(), e);
			return "";
		}
	}

}
