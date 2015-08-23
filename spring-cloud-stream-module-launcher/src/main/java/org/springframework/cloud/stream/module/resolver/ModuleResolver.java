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

package org.springframework.cloud.stream.module.resolver;

import org.springframework.core.io.Resource;

/**
 * Interface used to return a {@link Resource} that provides access to a module
 * uber-jar based on its Maven coordinates.
 *
 * @author David Turanski
 */
public interface ModuleResolver {

	/**
	 * Retrieve a resource given its coordinates.
	 *
	 * @param groupId the groupId
	 * @param artifactId the artifactId
	 * @param extension the file extension
	 * @param classifier classifier
	 * @param version the version
	 * @return the resource
	 */
	public Resource resolve(String groupId, String artifactId, String extension, String classifier, String version);

}
