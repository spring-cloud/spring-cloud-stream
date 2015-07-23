/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.module.registry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * A {@link ModuleRegistry} that delegates to several ModuleRegistries, in order.
 * @author Eric Bottard
 * @author Glenn Renfro
 * @author David Turanski
 */
public class DelegatingModuleRegistry implements WritableModuleRegistry {

	private List<ModuleRegistry> delegates = new ArrayList<ModuleRegistry>();

	public DelegatingModuleRegistry(ModuleRegistry... delegates) {
		setDelegates(delegates);
	}

	public DelegatingModuleRegistry() {
	}

	public void setDelegates(ModuleRegistry... delegates) {
		for (ModuleRegistry delegate : delegates) {
			this.delegates.add(delegate);
		}
	}

	@Override
	public ModuleDefinition findDefinition(String name, String moduleType) {
		for (ModuleRegistry delegate : delegates) {
			ModuleDefinition result = delegate.findDefinition(name, moduleType);
			if (result != null) {
				return result;
			}
		}
		return null;
	}

	@Override
	public List<ModuleDefinition> findDefinitionsByName(String name) {
		Set<String> alreadySeen = new HashSet<String>();
		List<ModuleDefinition> result = new ArrayList<ModuleDefinition>();
		for (ModuleRegistry delegate : delegates) {
			List<ModuleDefinition> sub = delegate.findDefinitionsByName(name);
			for (ModuleDefinition definition : sub) {
				// First registry's module shadows subsequent
				if (alreadySeen.add(makeKeyFor(definition))) {
					result.add(definition);
				}
			}
		}
		return result;
	}

	public void addDelegate(ModuleRegistry delegate) {
		delegates.add(delegate);
	}

	private String makeKeyFor(ModuleDefinition definition) {
		return definition.getType() + "|" + definition.getName();
	}

	@Override
	public List<ModuleDefinition> findDefinitionsByType(String type) {
		ArrayList<ModuleDefinition> definitions = new ArrayList<ModuleDefinition>();
		Set<String> alreadySeen = new HashSet<String>();
		for (ModuleRegistry delegate : delegates) {
			for (ModuleDefinition def : delegate.findDefinitionsByType(type)) {
				if (alreadySeen.add(makeKeyFor(def))) {
					definitions.add(def);
				}
			}
		}
		return definitions;
	}

	@Override
	public List<ModuleDefinition> findDefinitions() {
		ArrayList<ModuleDefinition> definitions = new ArrayList<ModuleDefinition>();
		Set<String> alreadySeen = new HashSet<String>();
		for (ModuleRegistry delegate : delegates) {
			for (ModuleDefinition def : delegate.findDefinitions()) {
				if (alreadySeen.add(makeKeyFor(def))) {
					definitions.add(def);
				}
			}
		}
		return definitions;
	}

	@Override
	public boolean delete(ModuleDefinition definition) {
		for (ModuleRegistry delegate : delegates) {
			if (delegate instanceof WritableModuleRegistry) {
				WritableModuleRegistry writableModuleRegistry = (WritableModuleRegistry) delegate;
				if (writableModuleRegistry.delete(definition)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean registerNew(ModuleDefinition definition) {
		for (ModuleRegistry delegate : delegates) {
			if (delegate instanceof WritableModuleRegistry) {
				WritableModuleRegistry writableModuleRegistry = (WritableModuleRegistry) delegate;
				if (writableModuleRegistry.registerNew(definition)) {
					return true;
				}
			}
		}
		return false;
	}
}
