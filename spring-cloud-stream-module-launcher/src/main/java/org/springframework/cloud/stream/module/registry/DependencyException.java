/*
 * Copyright 2013 the original author or authors.
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

import java.util.Set;


/**
 * Thrown when performing an action cannot be carried over because some dependency would be broken.
 * @author Eric Bottard
 */
@SuppressWarnings("serial")
public class /* Module? */DependencyException extends RuntimeException {

	private final Set<String> dependents;

	private final String name;

	private final String type;

	public DependencyException(String message, String name, String type, Set<String> dependents) {
		super(String.format(message, name, type, dependents));
		this.name = name;
		this.type = type;
		this.dependents = dependents;
	}

	public Set<String> getDependents() {
		return dependents;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

}
