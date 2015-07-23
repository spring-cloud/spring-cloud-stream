/*
 *
 *  * Copyright 2011-2014 the original author or authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.springframework.cloud.stream.module.registry;

import java.util.List;

/**
 * Factory class to create {@code ModuleDefinition}s.
 * @author Eric Bottard
 */
public class ModuleDefinitions {

	/**
	 * Not meant to be instantiated.
	 */
	private ModuleDefinitions() {

	}


	/**
	 * Create a new definition for a simple module (one where its implementation code can be located). Interpretation
	 * of the location of that code is left other class(es).
	 * @param groupId the groupId of the module
	 * @param artifactId the artifactId of the module
	 * @param version the version of the module
	 * @param location where the 'code' for the module can be found
	 */
	public static SimpleModuleDefinition simple(String groupId, String artifactId, String version,  String location) {
		return new SimpleModuleDefinition(groupId, artifactId, version, location);
	}

	/**
	 * Create a new definition for a composed module (one where its behavior comes not from its own 'code', but from
	 * composing pre-existing modules).
	 * @param groupId the name of the module
	 * @param artifactId the type of the module
	 * @param version the version of the module
	 * @param dslDefinition the definition that was used to create the module, in Spring XD DSL
	 * @param children information about the child modules that make up the composed module
	 */
	public static CompositeModuleDefinition composed(String groupId, String artifactId, String version,  String dslDefinition,
			List<ModuleDefinition> children) {
		return new CompositeModuleDefinition(groupId, artifactId, version, dslDefinition, children);
	}

}
