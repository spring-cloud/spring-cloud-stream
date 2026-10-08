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

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.cloud.bus.trace.InMemoryTraceRepository;
import org.springframework.cloud.bus.trace.Trace;
import org.springframework.cloud.bus.trace.TraceRepository;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link TraceBusEndpoint}.
 *
 * @author Ngoc Nhan
 */
class TraceBusEndpointTests {

	@Test
	void busTrace() {

		Trace trace = new Trace(Instant.now(), "spring.cloud.bus.ack", "type");
		trace.setId("id");
		trace.setOrigin("origin");
		trace.setDestination("destination");

		TraceRepository repository = new InMemoryTraceRepository();
		repository.add(trace);
		List<Trace> traces = new TraceBusEndpoint(repository).busTrace().getTraces();
		assertThat(traces).hasSize(1);

		Trace busTrace = traces.get(0);
		assertThat(busTrace.getSignal()).isEqualTo("spring.cloud.bus.ack");
	}

}
