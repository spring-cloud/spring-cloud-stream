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
	 * Lookup a module of the given type with the given name.
	 * @return {@code null} if this registry does not have such a module
	 */
	ModuleDefinition findDefinition(String name, String moduleType);

	/**
	 * Searches the registry for the name specified and returns all module definitions that match the name regardless
	 * of
	 * module type.
	 * @param name The module definition name to be searched.
	 * @return A list of the module definitions that have the name specified by the input parameter. If no module
	 * definition is found with the name an empty list will be returned.
	 */
	List<ModuleDefinition> findDefinitionsByName(String name);

	/**
	 * Searches the registry for the type specified and returns all module definitions that match the type.
	 * @param type The module type name to be searched.
	 * @return A list of the module definitions that have the type specified by the input parameter. If no module
	 * definition is found with the type an empty list will be returned.
	 */
	List<ModuleDefinition> findDefinitionsByType(String type);

	/**
	 * Returns all module definitions.
	 * @return A list of the module definitions. If no module definition is found with the type an empty list will be
	 * returned.
	 */
	List<ModuleDefinition> findDefinitions();

}
