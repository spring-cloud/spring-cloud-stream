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

package org.springframework.cloud.bus.endpoint;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.Selector.Match;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.cloud.bus.event.Destination;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.StringUtils;

/**
 * @author Spencer Gibb
 */
@Endpoint(id = "busrefresh") // TODO: document new id
public class RefreshBusEndpoint extends AbstractBusEndpoint {

	public RefreshBusEndpoint(ApplicationEventPublisher publisher, String id, Destination.Factory destinationFactory) {
		super(publisher, id, destinationFactory);
	}

	@WriteOperation
	public void busRefreshWithDestination(@Selector(match = Match.ALL_REMAINING) String[] destinations) {
		String destination = StringUtils.arrayToDelimitedString(destinations, ":");
		publish(new RefreshRemoteApplicationEvent(this, getInstanceId(), getDestination(destination)));
	}

	@WriteOperation
	public void busRefresh() {
		publish(new RefreshRemoteApplicationEvent(this, getInstanceId(), getDestination(null)));
	}

}
