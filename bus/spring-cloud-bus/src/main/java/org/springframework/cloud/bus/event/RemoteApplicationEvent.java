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

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.springframework.context.ApplicationEvent;
import org.springframework.core.style.ToStringCreator;
import org.springframework.util.Assert;

/**
 * @author Spencer Gibb
 */
@SuppressWarnings("serial")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties("source")
public abstract class RemoteApplicationEvent extends ApplicationEvent {

	private static final Object TRANSIENT_SOURCE = new Object();

	private static final String TRANSIENT_ORIGIN = "____transient_origin_service___";

	private static final String TRANSIENT_DESTINATION = "____transient_destination___";

	protected static final PathDestinationFactory DEFAULT_DESTINATION_FACTORY = new PathDestinationFactory();

	private final String originService;

	private final String destinationService;

	private final String id;

	protected RemoteApplicationEvent() {
		// for serialization libs like jackson
		this(TRANSIENT_SOURCE, TRANSIENT_ORIGIN, DEFAULT_DESTINATION_FACTORY.getDestination(TRANSIENT_DESTINATION));
	}

	@Deprecated
	protected RemoteApplicationEvent(Object source, String originService, String destinationService) {
		this(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(destinationService));
	}

	protected RemoteApplicationEvent(Object source, String originService, Destination destination) {
		super(source);
		if (!originService.equals(TRANSIENT_ORIGIN)) {
			Assert.notNull(originService, "originService may not be null");
			this.originService = originService;
		}
		else {
			this.originService = null;
		}
		Assert.notNull(destination, "destination may not be null");
		this.destinationService = destination.getDestinationAsString();
		Assert.hasText(destinationService, "destinationService may not be empty");
		this.id = UUID.randomUUID().toString();
	}

	@Deprecated
	protected RemoteApplicationEvent(Object source, String originService) {
		this(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(null));
	}

	public String getOriginService() {
		return this.originService;
	}

	public String getDestinationService() {
		return this.destinationService;
	}

	public String getId() {
		return this.id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.destinationService == null) ? 0 : this.destinationService.hashCode());
		result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
		result = prime * result + ((this.originService == null) ? 0 : this.originService.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		RemoteApplicationEvent other = (RemoteApplicationEvent) obj;
		if (this.destinationService == null) {
			if (other.destinationService != null) {
				return false;
			}
		}
		else if (!this.destinationService.equals(other.destinationService)) {
			return false;
		}
		if (this.id == null) {
			if (other.id != null) {
				return false;
			}
		}
		else if (!this.id.equals(other.id)) {
			return false;
		}
		if (this.originService == null) {
			if (other.originService != null) {
				return false;
			}
		}
		else if (!this.originService.equals(other.originService)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return new ToStringCreator(this).append("id", id).append("originService", originService)
				.append("destinationService", destinationService).toString();

	}

}
