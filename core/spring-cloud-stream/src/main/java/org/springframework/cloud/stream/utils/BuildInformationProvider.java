/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.cloud.stream.utils;


/**
 * Provides information about current Spring Cloud Stream build.
 *
 * @author Omer Celik
 */
public final class BuildInformationProvider {

	private static final String UNKNOWN_SCST_VERSION = "-1";

	private static final BuildInformation BUILD_INFO_CACHE;

	static {
		BUILD_INFO_CACHE = createBuildInformation();
	}

	private BuildInformationProvider() {
	}

	public static boolean isVersionValid() {
		return !getVersion().equals(UNKNOWN_SCST_VERSION);
	}
	public static boolean isVersionValid(String version) {
		return !version.equals(UNKNOWN_SCST_VERSION);
	}
	public static String getVersion() {
		return BUILD_INFO_CACHE.version();
	}

	// If you have a compilation error at GeneratedBuildProperties then run 'mvn clean install'
	// the GeneratedBuildProperties class is generated at a compile-time
	private static BuildInformation createBuildInformation() {
		return new BuildInformation(calculateVersion(), GeneratedBuildProperties.TIMESTAMP);
	}

	// If you have a compilation error at GeneratedBuildProperties then run 'mvn clean install'
	// the GeneratedBuildProperties class is generated at a compile-time
	private static String calculateVersion() {
		String version = GeneratedBuildProperties.VERSION;
		if (version.startsWith("@") && version.endsWith("@")) {
			return UNKNOWN_SCST_VERSION;
		}
		return version;
	}

	private record BuildInformation(String version, String timestamp) {
	}
}
