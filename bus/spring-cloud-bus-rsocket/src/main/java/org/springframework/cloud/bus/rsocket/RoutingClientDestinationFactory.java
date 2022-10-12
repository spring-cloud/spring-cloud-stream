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

import java.util.ArrayList;

import org.springframework.cloud.bus.event.Destination;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

public class RoutingClientDestinationFactory implements Destination.Factory {

	private final BusRSocketProperties properties;

	public RoutingClientDestinationFactory(BusRSocketProperties properties) {
		this.properties = properties;
	}

	@Override
	public Destination getDestination(String originalDestination) {
		ArrayList<String> entries = new ArrayList<>();
		properties.getDefaultTags().forEach((key, s) -> {
			String keyStr = (key.getWellKnownKey() != null) ? key.getWellKnownKey().name() : key.getKey();
			entries.add(keyStr + "=" + s);
		});
		String defaultTags = StringUtils.collectionToDelimitedString(entries, ":");
		return () -> {
			String destination = (ObjectUtils.isEmpty(originalDestination)) ? defaultTags
					: defaultTags + ":" + originalDestination;
			if (ObjectUtils.isEmpty(destination)) {
				throw new IllegalArgumentException("destination may not be empty");
			}
			return destination;
		};
	}

}
