/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Contains the properties of a binding.
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@JsonInclude(value = Include.NON_DEFAULT)
public class BindingProperties {

	private static final String COMMA = ",";

	/**
	 * The physical name at the broker that the binder binds to.
	 */
	private String destination;

	/**
	 * Unique name that the binding belongs to (applies to consumers only). Multiple consumers within the same group
	 * share the subscription. A null or empty String value indicates an anonymous group that is not shared.
	 * @see org.springframework.cloud.stream.binder.Binder#bindConsumer(java.lang.String, java.lang.String,
	 * java.lang.Object, org.springframework.cloud.stream.binder.ConsumerProperties)
	 */
	private String group;

	// Properties for both inbound/outbound

	private String contentType;

	private String binder;

	/**
	 * Flag to indicate if the message header needs to be updated with the trackedProperties.
	 */
	private Boolean trackHistory;

	/**
	 * Comma separated list of binding properties to track.
	 * By default the properties such as the current thread name and 'timestamp' are added if the 'trackHistory` is
	 * enabled.
	 */
	private String trackedProperties = "all";

	public String getDestination() {
		return this.destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getBinder() {
		return binder;
	}

	public void setBinder(String binder) {
		this.binder = binder;
	}

	public Boolean isTrackHistory() {
		return this.trackHistory;
	}

	public void setTrackHistory(Boolean trackHistory) {
		this.trackHistory = trackHistory;
	}

	public String getTrackedProperties() {
		return this.trackedProperties;
	}

	public void setTrackedProperties(String trackedProperties) {
		this.trackedProperties = trackedProperties;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("destination=" + this.destination);
		sb.append(COMMA);
		sb.append("group=" + this.group);
		sb.append(COMMA);
		if (this.contentType != null) {
			sb.append("contentType=" + this.contentType);
			sb.append(COMMA);
		}
		if (this.binder != null) {
			sb.append("binder=" + this.binder);
			sb.append(COMMA);
		}
		if (this.trackHistory != null) {
			sb.append("trackHistory=" + this.trackHistory);
			sb.append(COMMA);
		}
		sb.deleteCharAt(sb.lastIndexOf(COMMA));
		return "BindingProperties{" + sb.toString() + "}";
	}

}
