/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder;

import java.util.Arrays;

/**
 * References one or more
 * {@link org.springframework.context.annotation.Configuration}-annotated classes which
 * provide a context definition which contains exactly one {@link Binder}, typically for a
 * given type of system (e.g. Rabbit, Kafka, Redis, etc.). An application may contain
 * multiple instances of a given {@link BinderType}, when connecting to multiple systems
 * of the same type.
 *
 * @author Marius Bogoevici
 */
public class BinderType {

	private final String defaultName;

	private final Class<?>[] configurationClasses;

	public BinderType(String defaultName, Class<?>[] configurationClasses) {
		this.defaultName = defaultName;
		this.configurationClasses = configurationClasses;
	}

	public String getDefaultName() {
		return this.defaultName;
	}

	public Class<?>[] getConfigurationClasses() {
		return this.configurationClasses;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BinderType that = (BinderType) o;
		if (!this.defaultName.equals(that.defaultName)) {
			return false;
		}
		return Arrays.equals(this.configurationClasses, that.configurationClasses);

	}

	@Override
	public int hashCode() {
		int result = this.defaultName.hashCode();
		result = 31 * result + Arrays.hashCode(this.configurationClasses);
		return result;
	}

}
