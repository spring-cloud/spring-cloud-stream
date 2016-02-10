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

package org.springframework.cloud.stream.binder.gemfire;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class to keep track of consumer groups and the number of instances.
 * This class is not thread safe.
 *
 * @author Patrick Peralta
 */
public class ConsumerGroupTracker implements Serializable {
	private final Map<String, Integer> groupCounts = new HashMap<>();

	/**
	 * Add a consumer group instance. If the consumer group
	 * is already present, increment the instance count.
	 *
	 * @param group consumer group name
	 * @return number of instances of consumer group
	 */
	public int addGroup(String group) {
		Integer i = this.groupCounts.get(group);
		i = (i == null ? 1 : i + 1);
		this.groupCounts.put(group, i);
		return i;
	}

	/**
	 * Remove a consumer group instance.
	 *
	 * @param group consumer group name
	 * @return number of remaining instances
	 */
	public int removeGroup(String group) {
		Integer i = this.groupCounts.get(group);
		if (i == null) {
			return 0;
		}
		i = i - 1;
		if (i == 0) {
			this.groupCounts.remove(group);
		}
		else {
			this.groupCounts.put(group, i);
		}
		return i;
	}

	/**
	 * Return a set of consumer group names.
	 *
	 * @return unmodifiable set of consumer group names
	 */
	public Set<String> groups() {
		return Collections.unmodifiableSet(groupCounts.keySet());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ConsumerGroupTracker that = (ConsumerGroupTracker) o;
		return this.groupCounts.equals(that.groupCounts);
	}

	@Override
	public int hashCode() {
		return groupCounts.hashCode();
	}
}
