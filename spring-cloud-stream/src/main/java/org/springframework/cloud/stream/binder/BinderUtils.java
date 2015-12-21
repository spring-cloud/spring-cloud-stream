/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

/**
 * Binder utilities.
 *
 * @author Gary Russell
 * @author Mark Fisher
 */
public class BinderUtils {

	/**
	 * The delimiter between a group and index when constructing a binder consumer/producer.
	 */
	public static final String GROUP_INDEX_DELIMITER = ".";

	/**
	 * Construct a name comprised of the name and group.
	 * @param name the name.
	 * @param group the group.
	 * @return the constructed name.
	 */
	public static String groupedName(String name, String group) {
		return name + BinderUtils.GROUP_INDEX_DELIMITER + group;
	}

}
