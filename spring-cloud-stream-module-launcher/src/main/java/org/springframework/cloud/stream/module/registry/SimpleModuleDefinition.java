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

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A module definition for a "simple", single module whose code and supporting resources
 * can be located somewhere.
 * @author Eric Bottard
 */
public class SimpleModuleDefinition extends ModuleDefinition {

	private String location;

	@SuppressWarnings("unused")
	private SimpleModuleDefinition() {
		// For json de-serialization
	}

	/*package*/ SimpleModuleDefinition(String name, String moduleType, String location) {
		super(name, moduleType);
		this.location = location;
	}

	@Override
	public boolean isComposed() {
		return false;
	}

	// Don't bother serializing locations as these may not be portable across containers.
	// Will be looked-up again
	@JsonIgnore
	public String getLocation() {
		return location;
	}

	@Override
	public String toString() {
		return String.format("%s (%s:%s) @ %s", getClass().getSimpleName(), getType(), getName(), location);
	}
}
