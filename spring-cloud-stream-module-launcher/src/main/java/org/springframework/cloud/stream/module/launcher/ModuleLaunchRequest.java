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

package org.springframework.cloud.stream.module.launcher;

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Assert;

/**
 * Encapsulates a reference to a module (as maven coordinates) and a set of "arguments" that must be passed to it.
 * Those arguments will eventually be passed to the module by the launcher, using any way appropriate.
 *
 * @author Eric Bottard
 */
public class ModuleLaunchRequest {

	private final String module;

	private Map<String, String> arguments = new HashMap<>();

	public ModuleLaunchRequest(String module) {
		this.module = module;
	}

	public String getModule() {
		return module;
	}

	public Map<String, String> getArguments() {
		return arguments;
	}

	public void setArguments(Map<String, String> arguments) {
		Assert.notNull(arguments, "provided arguments Map must not be null");
		this.arguments = new HashMap<>(arguments);
	}

	public void addArgument(String name, String value) {
		this.arguments.put(name, value);
	}

	@Override
	public String toString() {
		return String.format("%s with arguments %s", module, arguments);
	}
}
