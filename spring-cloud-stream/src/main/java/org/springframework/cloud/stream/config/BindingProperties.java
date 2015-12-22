/*
 * Copyright 2015 the original author or authors.
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

import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Contains the properties of a binding.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
@JsonInclude(value = Include.NON_DEFAULT)
public class BindingProperties {

	private static final String COMMA = ",";

	private String destination;

	private String group = UUID.randomUUID().toString();

	private String contentType;

	private String binder;

	/**
	 *  Config properties thar are set per-binding level (like producer/consumer properties per-binding).
	 */
	private Map<String, String> config;

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

	public Map<String, String> getConfig() {
		return this.config;
	}

	public void setConfig(Map<String, String> properties) {
		this.config = properties;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("destination=" + destination);
		sb.append(COMMA);
		sb.append("group=" + group);
		sb.append(COMMA);
		sb.append("contentType=" + contentType);
		sb.append(COMMA);
		sb.append("binder=" + binder);
		sb.append(COMMA);
		if (config != null) {
			StringBuilder props = new StringBuilder();
			props.append("Properties{");
			for (Map.Entry<String, String> entry : config.entrySet()) {
				props.append(String.format("%s=%s,", entry.getKey(), entry.getValue()));
			}
			if (props.lastIndexOf(COMMA) > 0) {
				props.deleteCharAt(props.lastIndexOf(COMMA));
			}
			props.append("}");
			sb.append(props);
		}
		else {
			sb.deleteCharAt(sb.lastIndexOf(COMMA));
		}
		sb.deleteCharAt(sb.lastIndexOf(COMMA));
		return "BindingProperties{" + sb.toString() + "}";
	}

}
