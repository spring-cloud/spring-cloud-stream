/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.tuple;

/**
 * Extension of {@link Tuple} that allows mutating and addition of fields.
 *
 * @author Eric Bottard
 */
public interface MutableTuple extends Tuple {

	/**
	 * Sets the value of an already existing property, given its index.
	 */
	void setValue(int index, Object value);

	/**
	 * Sets the value of a property, by name. If this Tuple does not currently hold
	 * a property under that name, a new mapping is added at the end and the size of this
	 * Tuple is extended by 1.
	 */
	void setValue(String name, Object value);

}
