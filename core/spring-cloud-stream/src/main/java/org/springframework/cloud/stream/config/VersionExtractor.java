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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.stream.function.FunctionConfiguration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.StringUtils;
/**
 *
 * @author Oleg Zhurakousky
 * @since 4.2.x
 */
class VersionExtractor implements EnvironmentPostProcessor {

	protected final Log logger = LogFactory.getLog(getClass());

	private final Pattern versionPattern = Pattern.compile("(\\d+)\\.{1}(\\d+)\\.{1}(\\d+)(?:\\-{1}(\\w+))?");

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		String streamJarPath = FunctionConfiguration.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (logger.isDebugEnabled()) {
			logger.debug("Spring Cloud Stream is loaded from: " + streamJarPath);
		}
		System.setProperty("spring-cloud-stream.version", this.extractVersion(streamJarPath));
		String functionJarPath = FunctionCatalog.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (logger.isDebugEnabled()) {
			logger.debug("Spring Cloud Function is loaded from: " + streamJarPath);
		}
		System.setProperty("spring-cloud-function.version", this.extractVersion(functionJarPath));
	}

	private String extractVersion(String jarFilePath) {
		try {
			if (jarFilePath.endsWith(".jar")) {
				Matcher m = versionPattern.matcher(jarFilePath);
				if (!m.find()) {
					return "";
				}
				StringBuilder versionBuilder = new StringBuilder();
				String delimiter = "";
				for (int i = 0; i < m.groupCount();) {
					String value = m.group(++i);
					if (StringUtils.hasText(value)) {
						versionBuilder.append(delimiter);
						versionBuilder.append(value);
						delimiter = (i + 1 < m.groupCount()) ? "." : "-";
					}
				}
				return versionBuilder.toString();
			}
		}
		catch (Throwable e) {
			logger.warn("Failed to determine version of: " + jarFilePath, e);
		}
		return "";
	}
}
