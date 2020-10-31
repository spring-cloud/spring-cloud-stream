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

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.common.Key;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.RoutingType;

import org.springframework.cloud.bus.BusBridge;
import org.springframework.cloud.bus.BusConstants;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;
import org.springframework.util.StringUtils;

public class RSocketRequesterBusBridge implements BusBridge {

	public RSocketRequesterBusBridge(RoutingRSocketRequester requester) {
		this.requester = requester;
	}

	private final RoutingRSocketRequester requester;

	@Override
	public void send(RemoteApplicationEvent event) {
		requester.route(BusConstants.BUS_CONSUMER).address(builder -> {
			builder.routingType(RoutingType.MULTICAST);
			// get tags out of destination
			getTagsFromDestination(event.getDestinationService()).forEach(builder::with);
		}).data(event).send().subscribe();
	}

	static Map<Key, String> getTagsFromDestination(String delimitedProperties) {
		String[] properties = StringUtils.tokenizeToStringArray(delimitedProperties, ":");
		Map<Key, String> map = new HashMap<>();
		for (String property : properties) {
			int index = lowestIndexOf(property, "=");
			String key = (index > 0) ? property.substring(0, index) : property;
			String value = (index > 0) ? property.substring(index + 1) : null;

			try {
				WellKnownKey wellKnownKey = WellKnownKey.valueOf(key);
				map.put(Key.of(wellKnownKey), value);
			}
			catch (IllegalArgumentException e) {
				try {
					WellKnownKey wellKnownKey = WellKnownKey.valueOf(key.toUpperCase());
					map.put(Key.of(wellKnownKey), value);
				}
				catch (IllegalArgumentException e2) {
					// not a WellKnownKey, use string
					map.put(Key.of(key), value);
				}
			}

		}
		return map;
	}

	private static int lowestIndexOf(String property, String... candidates) {
		int index = -1;
		for (String candidate : candidates) {
			int candidateIndex = property.indexOf(candidate);
			if (candidateIndex > 0) {
				index = (index != -1) ? Math.min(index, candidateIndex) : candidateIndex;
			}
		}
		return index;
	}

}
