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

import org.springframework.util.Assert;

/**
 * Thrown when attempting to refer to a module that does not exist.
 * @author Eric Bottard
 * @author David Turanski
 */
@SuppressWarnings("serial")
public class NoSuchModuleException extends ResourceDefinitionException {

	private final String artifactId;

	private final String groupId;

	private final String version;


	/**
	 * Create a new exception.
	 * @param groupId the module groupId that was referenced, but could not be found
	 * @param artifactId the module artifactId that was referenced, but could not be found
	 * @param version the version
	 */
	public NoSuchModuleException(String groupId, String artifactId, String version) {
		super(""); // will be overriden by getMessage()
		Assert.hasText(groupId);
		Assert.hasText(artifactId);

		this.artifactId = artifactId;
		this.groupId = groupId;
		this.version = version;
	}

	@Override
	public String getMessage() {
		return String.format("Could not find module '%s:%s:%s'", groupId
				, artifactId, version);
	}

}
