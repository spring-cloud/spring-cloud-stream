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

package org.springframework.cloud.bus.event;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
public class RefreshRemoteApplicationEvent extends RemoteApplicationEvent {

	@SuppressWarnings("unused")
	private RefreshRemoteApplicationEvent() {
		// for serializers
	}

	@Deprecated
	public RefreshRemoteApplicationEvent(Object source, String originService, String destination) {
		this(source, originService, new PathDestinationFactory().getDestination(destination));
	}

	public RefreshRemoteApplicationEvent(Object source, String originService, Destination destination) {
		super(source, originService, destination);
	}

}
