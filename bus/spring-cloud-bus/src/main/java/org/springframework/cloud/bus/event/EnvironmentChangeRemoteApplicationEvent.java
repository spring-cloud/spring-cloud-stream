/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus.event;

import java.util.Map;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
public class EnvironmentChangeRemoteApplicationEvent extends RemoteApplicationEvent {

	private final Map<String, String> values;

	@SuppressWarnings("unused")
	private EnvironmentChangeRemoteApplicationEvent() {
		// for serializers
		values = null;
	}

	public EnvironmentChangeRemoteApplicationEvent(Object source, String originService,
			String destinationService, Map<String, String> values) {
		super(source, originService, destinationService);
		this.values = values;
	}

	public Map<String, String> getValues() {
		return values;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EnvironmentChangeRemoteApplicationEvent other = (EnvironmentChangeRemoteApplicationEvent) obj;
		if (values == null) {
			if (other.values != null)
				return false;
		}
		else if (!values.equals(other.values))
			return false;
		return true;
	}

}
