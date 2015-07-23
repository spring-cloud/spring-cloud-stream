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

import java.util.Arrays;

import org.springframework.util.Assert;

/**
 * Thrown when attempting to refer to a module that does not exist.
 * @author Eric Bottard
 */
@SuppressWarnings("serial")
public class NoSuchModuleException extends ResourceDefinitionException {

	private final String name;

	private final String[] candidateTypes;

	/**
	 * Create a new exception.
	 * @param name the module name that was referenced, but could not be found
	 * @param candidateTypes the type(s) of the module that was expected but could not be found
	 */
	public NoSuchModuleException(String name, String... candidateTypes) {
		super(""); // will be overriden by getMessage()
		Assert.hasText(name);
		Assert.notEmpty(candidateTypes);
		this.name = name;
		this.candidateTypes = candidateTypes;
	}

	@Override
	public String getMessage() {
		if (candidateTypes.length == 1) {
			return String.format("Could not find module with name '%s' and type '%s'", name,
					candidateTypes[0]);
		}
		else {
			return String.format("Could not find module with name '%s' and type among %s", name,
					Arrays.asList(candidateTypes));
		}
	}

	public String getName() {
		return name;
	}


	public String[] getCandidateTypes() {
		return candidateTypes;
	}
}
