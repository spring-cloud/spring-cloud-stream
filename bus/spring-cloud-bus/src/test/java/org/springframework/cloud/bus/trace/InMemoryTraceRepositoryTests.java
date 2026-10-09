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

package org.springframework.cloud.bus.trace;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link InMemoryTraceRepository}.
 *
 * @author Ngoc Nhan
 */
public class InMemoryTraceRepositoryTests {

	private final InMemoryTraceRepository repository = new InMemoryTraceRepository();

	@Test
	void adWhenHasLimitedCapacityRestrictsSize() {

		this.repository.setCapacity(2);

		for (Trace trace : this.createTraces()) {
			this.repository.add(trace);
		}
		List<Trace> trace = this.repository.findAll();

		assertThat(trace).hasSize(2);
		assertThat(trace.get(0).getType()).isEqualTo("type3");
		assertThat(trace.get(1).getType()).isEqualTo("type2");
	}

	@Test
	void addWhenReverseFalseReturnsInCorrectOrder() {

		this.repository.setReverse(false);
		this.repository.setCapacity(2);

		for (Trace trace : this.createTraces()) {
			this.repository.add(trace);
		}
		List<Trace> trace = this.repository.findAll();

		assertThat(trace).hasSize(2);
		assertThat(trace.get(0).getType()).isEqualTo("type2");
		assertThat(trace.get(1).getType()).isEqualTo("type3");
	}

	private List<Trace> createTraces() {

		Trace trace1 = new Trace(Instant.now(), "spring.cloud.bus.ack", "type1");
		trace1.setId("id1");
		trace1.setOrigin("origin1");
		trace1.setDestination("destination1");

		Trace trace2 = new Trace(Instant.now(), "spring.cloud.bus.sent", "type2");
		trace2.setId("id2");
		trace2.setOrigin("origin2");
		trace2.setDestination("destination2");

		Trace trace3 = new Trace(Instant.now(), "spring.cloud.bus.ack", "type3");
		trace3.setId("id3");
		trace3.setOrigin("origin3");
		trace3.setDestination("destination3");
		return List.of(trace1, trace2, trace3);
	}

}
