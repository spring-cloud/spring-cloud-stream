/*
 * Copyright 2015-2020 the original author or authors.
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

package org.springframework.cloud.bus.rsocket;

import java.util.HashMap;
import java.util.Map;

import io.rsocket.routing.client.spring.RoutingClientProperties;
import io.rsocket.routing.common.Key;
import io.rsocket.routing.common.WellKnownKey;

import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.util.AntPathMatcher;

import static org.springframework.cloud.bus.rsocket.RSocketRequesterBusBridge.getTagsFromDestination;

/**
 * A pass thru patcher that allows the RSocket Routing broker to determine which instances
 * to send to.
 */
public class RSocketServiceMatcher implements ServiceMatcher {

	private final String busId;

	private final RoutingClientProperties properties;

	private final AntPathMatcher antPathMatcher = new AntPathMatcher();

	private final Map<Key, String> localTags = new HashMap<>();

	public RSocketServiceMatcher(String busId, RoutingClientProperties properties) {
		this.busId = busId;
		this.properties = properties;
		convertLocalTags(properties);
	}

	@Override
	public boolean isFromSelf(RemoteApplicationEvent event) {
		String originService = event.getOriginService();
		String serviceId = getBusId();
		return antPathMatcher.match(originService, serviceId);
	}

	@Override
	public boolean isForSelf(RemoteApplicationEvent event) {
		Map<Key, String> tags = getTagsFromDestination(event.getDestinationService());
		for (Map.Entry<Key, String> entry : tags.entrySet()) {
			String existingValue = localTags.get(entry.getKey());
			if (existingValue == null || !existingValue.equals(entry.getValue())) {
				return false;
			}
		}
		return true;
	}

	private void convertLocalTags(RoutingClientProperties properties) {
		properties.getTags().forEach((key, value) -> {
			if (key.getWellKnownKey() != null) {
				localTags.put(Key.of(key.getWellKnownKey()), value);
			}
			else {
				localTags.put(Key.of(key.getKey()), value);
			}
		});
		localTags.put(Key.of(WellKnownKey.SERVICE_NAME), properties.getServiceName());
	}

	@Override
	public String getBusId() {
		return busId;
	}

}
