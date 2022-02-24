/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.micrometer;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@JsonPropertyOrder({ "name", "inteval", "createdTime", "properties", "metrics" })
class ApplicationMetrics {

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
	private final Date createdTime;

	private String name;

	private long interval;

	private Collection<Metric<Number>> metrics;

	private Map<String, Object> properties;

	@JsonCreator
	ApplicationMetrics(@JsonProperty("name") String name,
			@JsonProperty("metrics") Collection<Metric<Number>> metrics) {
		this.name = name;
		this.metrics = metrics;
		this.createdTime = new Date();
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Collection<Metric<Number>> getMetrics() {
		return this.metrics;
	}

	public void setMetrics(Collection<Metric<Number>> metrics) {
		this.metrics = metrics;
	}

	public Date getCreatedTime() {
		return this.createdTime;
	}

	public Map<String, Object> getProperties() {
		return this.properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	public long getInterval() {
		return this.interval;
	}

	public void setInterval(long interval) {
		this.interval = interval;
	}

}
