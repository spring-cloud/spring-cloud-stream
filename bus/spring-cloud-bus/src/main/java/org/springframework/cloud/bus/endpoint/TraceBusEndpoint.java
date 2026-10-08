/*
 * Copyright 2012-present the original author or authors.
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

import java.util.List;

import org.springframework.boot.actuate.endpoint.OperationResponseBody;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.cloud.bus.trace.Trace;
import org.springframework.cloud.bus.trace.TraceRepository;
import org.springframework.util.Assert;

/**
 * {@link Endpoint @Endpoint} to expose {@link Trace} information.
 *
 * @author Ngoc Nhan
 * @since 5.1.0
 */
@Endpoint(id = "bustrace")
public class TraceBusEndpoint {

	private final TraceRepository repository;

	/**
	 * Create a new {@link TraceBusEndpoint} instance.
	 * @param repository the trace repository
	 */
	public TraceBusEndpoint(TraceRepository repository) {
		Assert.notNull(repository, "'repository' must not be null");
		this.repository = repository;
	}

	/**
	 * Description of an application's {@link Trace} entries.
	 */
	@ReadOperation
	public TraceDescriptor busTrace() {
		return new TraceDescriptor(this.repository.findAll());
	}

	public static final class TraceDescriptor implements OperationResponseBody {

		private final List<Trace> traces;

		private TraceDescriptor(List<Trace> traces) {
			this.traces = traces;
		}

		public List<Trace> getTraces() {
			return this.traces;
		}

	}

}
