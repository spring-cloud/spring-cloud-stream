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

/**
 * Base interface for registries that know how to create (and hence how to delete) new module definitions.
 * @author Eric Bottard
 */
public interface WritableModuleRegistry extends ModuleRegistry {

	/**
	 * Attempt to delete the given definition, if that registry is responsible for it.
	 * @return true if this registry was responsible for the definition and it was actually deleted
	 */
	public boolean delete(ModuleDefinition definition);

	/**
	 * Attempt to register the given definition, if that registry can handle it.
	 * @return true if this registry is able to register the new definition and actually did
	 */
	public boolean registerNew(ModuleDefinition definition);
}
