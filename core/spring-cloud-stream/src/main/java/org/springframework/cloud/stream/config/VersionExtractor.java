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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.stream.function.FunctionConfiguration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.StringUtils;
/**
 * {@link EnvironmentPostProcessor} that contributes the Spring Cloud Stream banner.
 * <p>
 * The banner is stored outside of the default location Spring Boot looks at
 * ({@code classpath:banner.txt}), so it can never compete with an application's own
 * banner. It is activated by pointing {@code spring.banner.location} to it, and only when
 * all of the following is true:
 * <ul>
 * <li>{@code spring.cloud.stream.banner.enabled} is not set to {@code false}
 * (default is {@code true});</li>
 * <li>the banner is not turned off via {@code spring.main.banner-mode=off};</li>
 * <li>the application did not configure {@code spring.banner.location};</li>
 * <li>the application does not provide its own {@code banner.txt} or image banner in
 * the default location.</li>
 * </ul>
 * Otherwise nothing is done, including the extraction of the version numbers that are
 * only needed to render the banner.
 *
 * @author Oleg Zhurakousky
 * @author Nicolo Pietro Belcastro
 * @since 4.2.x
 */
class VersionExtractor implements EnvironmentPostProcessor {

	/**
	 * Property to disable the Spring Cloud Stream banner.
	 */
	static final String BANNER_ENABLED_PROPERTY = "spring.cloud.stream.banner.enabled";

	static final String BANNER_LOCATION = "classpath:org/springframework/cloud/stream/banner.txt";

	private static final String BOOT_BANNER_LOCATION_PROPERTY = "spring.banner.location";

	private static final String BOOT_BANNER_MODE_PROPERTY = "spring.main.banner-mode";

	private static final String BANNER_PROPERTY_SOURCE_NAME = "springCloudStreamBanner";

	private static final String[] APPLICATION_BANNERS = { "banner.txt", "banner.gif", "banner.jpg", "banner.png" };

	protected final Log logger = LogFactory.getLog(getClass());

	private final Pattern versionPattern = Pattern.compile("(\\d+)\\.{1}(\\d+)\\.{1}(\\d+)(?:\\-{1}(\\w+))?");

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		if (!shouldContributeBanner(environment, application)) {
			return;
		}
		environment.getPropertySources().addLast(new MapPropertySource(BANNER_PROPERTY_SOURCE_NAME,
				Map.of(BOOT_BANNER_LOCATION_PROPERTY, BANNER_LOCATION)));
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

	private boolean shouldContributeBanner(ConfigurableEnvironment environment, SpringApplication application) {
		if (!environment.getProperty(BANNER_ENABLED_PROPERTY, Boolean.class, true)) {
			return false;
		}
		if ("off".equalsIgnoreCase(environment.getProperty(BOOT_BANNER_MODE_PROPERTY))
				|| environment.containsProperty(BOOT_BANNER_LOCATION_PROPERTY)) {
			return false;
		}
		ResourceLoader resourceLoader = (application.getResourceLoader() != null) ? application.getResourceLoader()
				: new DefaultResourceLoader(application.getClassLoader());
		for (String banner : APPLICATION_BANNERS) {
			if (resourceLoader.getResource("classpath:" + banner).exists()) {
				if (logger.isDebugEnabled()) {
					logger.debug("Application provides its own '" + banner + "', Spring Cloud Stream banner is skipped");
				}
				return false;
			}
		}
		return true;
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
