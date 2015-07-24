/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.cloud.stream.module.registry;

import org.springframework.core.io.Resource;

/**
 * Retrieve
 * @author David Turanski
 */
public interface  ModuleResolver {
	/**
	 * Retrieve a resource given it's coordinates.
	 * @param groupId the groupId
	 * @param artifactId the artifactId
	 * @param version the version
	 * @return the resource
	 */
	public Resource locateResource(String groupId, String artifactId, String version);
}
