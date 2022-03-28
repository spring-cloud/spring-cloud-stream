/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;

/**
 * Immutable class that wraps the micrometer's {@link HistogramSnapshot}.
 *
 * @param <T> the value of type {@link Number}
 * @author Oleg Zhurakousky
 */
@JsonPropertyOrder({ "id", "timestamp", "sum", "count", "mean", "upper", "total" })
class Metric<T extends Number> {

	private final Date timestamp;

	private final Meter.Id id;

	private final Number sum;

	private final Number count;

	private final Number mean;

	private final Number upper;

	private final Number total;

	/**
	 * Create a new {@link Metric} instance.
	 * @param id Meter id
	 * @param snapshot instance of HistogramSnapshot
	 */
	Metric(Meter.Id id, HistogramSnapshot snapshot) {
		this.timestamp = new Date();
		this.id = id;
		this.sum = snapshot.total(TimeUnit.MILLISECONDS);
		this.count = snapshot.count();
		this.mean = snapshot.mean(TimeUnit.MILLISECONDS);
		this.upper = snapshot.max(TimeUnit.MILLISECONDS);
		this.total = snapshot.total(TimeUnit.MILLISECONDS);
	}

	public Meter.Id getId() {
		return this.id;
	}

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
	public Date getTimestamp() {
		return this.timestamp;
	}

	public Number getSum() {
		return this.sum;
	}

	public Number getCount() {
		return this.count;
	}

	public Number getMean() {
		return this.mean;
	}

	public Number getUpper() {
		return this.upper;
	}

	public Number getTotal() {
		return this.total;
	}

	@Override
	public String toString() {
		return "Metric [id=" + this.id + ", sum=" + this.sum + ", count=" + this.count
				+ ", mean=" + this.mean + ", upper=" + this.upper + ", total="
				+ this.total + ", timestamp=" + this.timestamp + "]";
	}

}
