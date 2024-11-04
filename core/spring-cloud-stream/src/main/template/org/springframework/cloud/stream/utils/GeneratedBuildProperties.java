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

import javax.annotation.processing.Generated;

/**
 * Exposes the Spring Cloud Stream Build Properties.
 * This class is generated in a build-time from a template stored at
 * src/main/template/org/springframework/cloud/stream/utils/GeneratedBuildProperties.
 *
 * Do not edit by hand as the changes will be overwritten in the next build.
 * We use this method to support all build types. (Fat jar, shaded jar, war, etc.)
 *
 * WARNING: DO NOT CHANGE FIELD VALUES IN THE TEMPLATE.(For example: @project.version@)
 * The fields are injected using the @.....@ keywords with the Maven Antrun Plugin.
 *
 * @author Omer Celik
 * @since 4.2.0
 */
@Generated("")
public final class GeneratedBuildProperties {

	/**
	 * Indicates the Spring Cloud Stream version.
	 */
	public static final String VERSION = "@project.version@";

	/**
	 * Indicates the build time of the project.
	 */
	public static final String TIMESTAMP = "@timestamp@";

	private GeneratedBuildProperties() {
	}
}
