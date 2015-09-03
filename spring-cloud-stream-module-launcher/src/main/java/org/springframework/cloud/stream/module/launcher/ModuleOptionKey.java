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

/**
 * Module option key that represents the module index (the index based on the program argument for the module launcher)
 * and the option key.
 *
 * @author Ilayaperumal Gopinathan
 */
public class ModuleOptionKey {

	private final int index;

	private final String option;

	public ModuleOptionKey(int index, String option) {
		this.index = index;
		this.option = option;
	}

	public int getIndex() {
		return index;
	}

	public String getOption() {
		return option;
	}

}
