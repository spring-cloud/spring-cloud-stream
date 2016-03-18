/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.bus.event;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * An event that signals an ack of a specific {@link RemoteApplicationEvent}. These events
 * can be monitored by any applications that want to audit the responses to bus events.
 * They behave like normal remote application events, in the sense that if the destination
 * service matches the local service ID the application fires the event in its context.
 *
 * @author Dave Syer
 *
 */
@SuppressWarnings("serial")
@Data
@EqualsAndHashCode(callSuper = false)
public class AckRemoteApplicationEvent extends RemoteApplicationEvent {

	private final String ackId;
	private final String ackDestinationService;
	private final Class<? extends RemoteApplicationEvent> event;

	@SuppressWarnings("unused")
	private AckRemoteApplicationEvent() {
		super();
		this.ackDestinationService = null;
		this.ackId = null;
		this.event = null;
	}

	public AckRemoteApplicationEvent(Object source, String originService,
			String destinationService, String ackDestinationService, String ackId,
			Class<? extends RemoteApplicationEvent> type) {
		super(source, originService, destinationService);
		this.ackDestinationService = ackDestinationService;
		this.ackId = ackId;
		this.event = type;
	}
}
