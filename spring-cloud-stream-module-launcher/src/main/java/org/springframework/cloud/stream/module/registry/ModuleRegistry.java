/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.cloud.stream.module.registry;

import java.util.List;

/**
 * A module registry is used to lookup modules by name and/or type. A non-null / non-empty
 * result indicates that a module exists, but further validation and introspection is left to
 * the caller.
 * @author Mark Fisher
 * @author Gary Russell
 * @author Glenn Renfro
 * @author David Turanski
 */
public interface ModuleRegistry {

	/**
	 * Look up a module definition
	 * @param groupId the grouo Id
	 * @param artifactId the artifact Id
	 * @param version the version
	 * @return the {@link ModuleDefinition}
	 */
	ModuleDefinition findModuleDefinition(String groupId, String artifactId, String version);

	/**
	 * Searches the registry for the artifactId specified and returns all versions of module definitions that match.
	 * @param groupId the groupId of the artifact to be searched
	 * @param artifactId The artifactId of module artifact to be searched
	 * @return A list of the module definitions . If no module is found, an empty list will be returned.
	 */
	List<ModuleDefinition> findVersionsForModuleDefinition(String groupId, String artifactId);

	/**
	 * Searches the registry for the groupId specified and returns all module definitions that match.
	 * @param groupId The module groupId to be searched.
	 * @return A list of the module definitions that contain the group. If no modules are 
	 * found, an empty list will be returned.
	 */
	List<ModuleDefinition> findModuleDefinitionsByGroupId(String groupId);

}
