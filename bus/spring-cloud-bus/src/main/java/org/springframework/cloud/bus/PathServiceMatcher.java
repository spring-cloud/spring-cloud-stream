/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus;

import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.util.PathMatcher;

/**
 * @author Spencer Gibb
 */
public class PathServiceMatcher implements ServiceMatcher {

	private final PathMatcher matcher;

	private final String id;

	private String[] configNames = new String[] {};

	public PathServiceMatcher(PathMatcher matcher, String id) {
		this.matcher = matcher;
		this.id = id;
	}

	public PathServiceMatcher(PathMatcher matcher, String id, String[] configNames) {
		this(matcher, id);

		int colonIndex = id.indexOf(":");
		if (colonIndex >= 0) {
			// if the id contains profiles and port, append them to the config names
			String profilesAndPort = id.substring(colonIndex);
			for (int i = 0; i < configNames.length; i++) {
				configNames[i] = configNames[i] + profilesAndPort;
			}
		}
		this.configNames = configNames;
	}

	public boolean isFromSelf(RemoteApplicationEvent event) {
		String originService = event.getOriginService();
		String serviceId = getBusId();
		return this.matcher.match(originService, serviceId);
	}

	public boolean isForSelf(RemoteApplicationEvent event) {
		String destinationService = event.getDestinationService();
		if (destinationService == null || destinationService.trim().isEmpty()
				|| this.matcher.match(destinationService, getBusId())) {
			return true;
		}

		// Check all potential config names instead of service name
		for (String configName : this.configNames) {
			if (this.matcher.match(destinationService, configName)) {
				return true;
			}
		}

		return false;
	}

	public String getBusId() {
		return this.id;
	}

}
