/*
 * Copyright 2014 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * Runtime {@link ModuleDeploymentProperties} for a module.
 * @author Patrick Peralta
 */
public class RuntimeModuleDeploymentProperties extends ModuleDeploymentProperties {

	/**
	 * Key for the {@code sequence} property. Value should be an integer.
	 */
	public static final String SEQUENCE_KEY = "sequence";

	/**
	 * Get the module sequence.
	 * @return the module sequence
	 */
	public int getSequence() {
		return Integer.parseInt(getSequenceAsString());
	}

	/**
	 * Get the module sequence as string.
	 * @return the string representation of the module sequence
	 */
	public String getSequenceAsString() {
		Assert.state(super.containsKey(SEQUENCE_KEY), "Missing sequence");
		return super.get(SEQUENCE_KEY);
	}

	/**
	 * Set the module sequence.
	 * @param sequence the module sequence
	 */
	public void setSequence(int sequence) {
		super.put(SEQUENCE_KEY, String.valueOf(sequence));
	}

}
