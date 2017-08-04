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

import com.fasterxml.jackson.annotation.JsonProperty;

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
public class AckRemoteApplicationEvent extends RemoteApplicationEvent {

	private final String ackId;
	private final String ackDestinationService;
	private Class<? extends RemoteApplicationEvent> event;

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

	public String getAckId() {
		return ackId;
	}

	public String getAckDestinationService() {
		return ackDestinationService;
	}

	public Class<? extends RemoteApplicationEvent> getEvent() {
		return event;
	}

	/**
	 * Used by Jackson to set the remote class name of the event implementation. If the implementing class is unknown to
	 * this app, set the event to {@link UnknownRemoteApplicationEvent}.
	 *
	 * @param eventName  the fq class name of the event implementation, not null
	 */
	@JsonProperty("event")
	public void setEventName(String eventName) {
		try {
			event = (Class<? extends RemoteApplicationEvent>) Class.forName(eventName);
		} catch (ClassNotFoundException e) {
			event = UnknownRemoteApplicationEvent.class;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((ackDestinationService == null) ? 0
				: ackDestinationService.hashCode());
		result = prime * result + ((ackId == null) ? 0 : ackId.hashCode());
		result = prime * result + ((event == null) ? 0 : event.hashCode());
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
		AckRemoteApplicationEvent other = (AckRemoteApplicationEvent) obj;
		if (ackDestinationService == null) {
			if (other.ackDestinationService != null)
				return false;
		}
		else if (!ackDestinationService.equals(other.ackDestinationService))
			return false;
		if (ackId == null) {
			if (other.ackId != null)
				return false;
		}
		else if (!ackId.equals(other.ackId))
			return false;
		if (event == null) {
			if (other.event != null)
				return false;
		}
		else if (!event.equals(other.event))
			return false;
		return true;
	}
}
