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


/**
 * Thrown when trying to create a new module with the given artifactId and groupId, but one already exists.
 * @author Eric Bottard
 */
@SuppressWarnings("serial")
public class ModuleAlreadyExistsException extends RuntimeException {

	private final String artifactId;

	private final String groupId;

	private final String version;

	/**
	 * Construct a new exception with the given parameters. The message template will be interpolated using
	 * {@link String#format(String, Object...)} with the module artifactId and groupId being the first and second
	 * replacements.
	 */
	public ModuleAlreadyExistsException(String messageTemplate, String groupId, String artifactId, String version) {
		super(String.format(messageTemplate, groupId, artifactId, version));
		this.groupId = groupId;
		this.artifactId = artifactId;
		this.version = version;
	}

	public ModuleAlreadyExistsException(String artifactId, String groupId, String version) {
		this("There is already a module '%s:%s:%s'", artifactId, groupId, version);
	}

	public String getArtifactId() {
		return artifactId;
	}

	public String getGroupId() {
		return groupId;
	}

	public String getVersion() { return version;  }

}
