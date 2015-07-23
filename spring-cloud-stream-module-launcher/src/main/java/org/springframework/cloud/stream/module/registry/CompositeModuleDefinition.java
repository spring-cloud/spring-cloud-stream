/*
 * Copyright 2011-2014 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * A module definition for a module which has been custom created as the composition of several other
 * modules (either {@link SimpleModuleDefinition simple} or
 * themselves {@link CompositeModuleDefinition composed}.
 * @author Eric Bottard
 */
public class CompositeModuleDefinition extends ModuleDefinition {

	private String dslDefinition;

	// See XD-2284; should be more than just definitions
	private List<ModuleDefinition> children;

	@SuppressWarnings("unused")
	private CompositeModuleDefinition() {
		// For json de-serialization
	}

	/*package*/ CompositeModuleDefinition(String name, String type,
			String dslDefinition, List<ModuleDefinition> children) {
		super(name, type);
		Assert.hasText(dslDefinition, "The dsl definition for a composed module must not be blank");
		Assert.notEmpty(children, "The sub-module definitions for a composed module must have at least one element");
		this.dslDefinition = dslDefinition;
		this.children = children;
	}

	@Override
	public boolean isComposed() {
		return true;
	}

	@Override
	public String toString() {
		return String.format("%s (%s:%s) [%s]", getClass().getSimpleName(), getType(), getName(), dslDefinition);
	}

	/**
	 * Return information about the sub-modules that make up this composed module.
	 */
	public List<ModuleDefinition> getChildren() {
		return children;
	}
}
