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

/**
 * Encapsulates a reference to a module (as maven coordinates) and a set of "arguments" that must be passed to it.
 * Those arguments will be passed to the module by the launcher.
 *
 * @author Eric Bottard
 * @author Ilayaperumal Gopinathan
 */
public class ModuleLaunchRequest {

	/**
	 * String that uniquely identifies each module launch request. This is useful when there are multiple requests
	 * with the same module that need to be uniquely identified.
	 */
	private final String requestId;

	private final String module;

	private final Map<String, String> arguments;

	public ModuleLaunchRequest(String requestId, String module, Map<String, String> arguments) {
		this.requestId = requestId;
		this.module = module;
		this.arguments = arguments != null ? new HashMap<>(arguments) : new HashMap<String, String>();
	}

	public String getRequestId() {
		return requestId;
	}

	public String getModule() {
		return module;
	}

	public Map<String, String> getArguments() {
		return arguments;
	}

	public void addArgument(String name, String value) {
		this.arguments.put(name, value);
	}

	@Override
	public String toString() {
		return String.format("%s with arguments %s", module, arguments);
	}
}
