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

package org.springframework.cloud.bus;

import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.util.PathMatcher;

/**
 * @author Spencer Gibb
 */
public class ServiceMatcher {
	private BusProperties context;
	private PathMatcher matcher;

	public void setBusProperties(BusProperties context) {
		this.context = context;
	}

	public void setMatcher(PathMatcher matcher) {
		this.matcher = matcher;
	}

	public boolean isFromSelf(RemoteApplicationEvent event) {
		String originService = event.getOriginService();
		String serviceId = getServiceId();
		return this.matcher.match(originService, serviceId);
	}

	public boolean isForSelf(RemoteApplicationEvent event) {
		String destinationService = event.getDestinationService();
		return (destinationService == null || destinationService.trim().isEmpty()
				|| this.matcher.match(destinationService, getServiceId()));
	}

	public String getServiceId() {
		return this.context.getId();
	}

}
