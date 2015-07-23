/*
 *
 *  * Copyright 2011-2014 the original author or authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.springframework.cloud.stream.module.registry;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * A module definition that serves as a temporary vehicle for the payload of an uploaded module.
 * @author Eric Bottard
 */
public class UploadedModuleDefinition extends ModuleDefinition {

	private final InputStream inputStream;

	public UploadedModuleDefinition(String name, String type, byte[] bytes) {
		this(name, type, new ByteArrayInputStream(bytes));
	}

	public UploadedModuleDefinition(String name, String moduleType, InputStream is) {
		super(name, moduleType);
		this.inputStream = is;
	}

	@Override
	public boolean isComposed() {
		return false;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	@Override
	public String toString() {
		return String.format("Uploaded module '%s:%s'", getType(), getName());
	}
}
