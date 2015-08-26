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

import java.util.Map;

import org.springframework.core.env.PropertySources;


/**
 * Encapsulates the operation of isolating arguments for different modules. Provides two operations that are the
 * inverse of each other. That is, the following pseudo invariants must hold:
 * <pre>
 *     props == unqualify("foo", qualify("foo", props))
 *     unqualify("foo", qualify("bar", props)) == empty
 * </pre>
 *
 * @author Eric Bottard
 */
public interface ArgumentsNamespacingStrategy {


	/**
	 * Given a set of environmental properties (will typically come from the
	 * Spring {@link org.springframework.core.env.Environment}), return a set of key/value pairs that should be
	 * used for a given module.
	 */
	Map<String, String> unqualify(String module, PropertySources propertySources);

	/**
	 * Transform a set of arguments for a given module to a form that is guaranteed to not interfere with
	 * other modules.
	 */
	Map<String, String> qualify(String module, Map<String, String> arguments);
}
