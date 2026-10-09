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

import java.util.LinkedList;
import java.util.List;

/**
 * In-memory implementation of {@link TraceRepository}.
 *
 * @author Ngoc Nhan
 * @since 5.1.0
 */
public class InMemoryTraceRepository implements TraceRepository {

	private final List<Trace> traces = new LinkedList<>();

	private int capacity = 100;

	private boolean reverse = true;

	/**
	 * Flag to say that the repository lists exchanges in reverse order.
	 * @param reverse flag value (default true)
	 */
	public void setReverse(boolean reverse) {
		synchronized (this.traces) {
			this.reverse = reverse;
		}
	}

	/**
	 * Set the capacity of the in-memory repository.
	 * @param capacity the capacity
	 */
	public void setCapacity(int capacity) {
		synchronized (this.traces) {
			this.capacity = capacity;
		}
	}

	@Override
	public List<Trace> findAll() {
		synchronized (this.traces) {
			return List.copyOf(this.traces);
		}
	}

	@Override
	public void add(Trace trace) {
		synchronized (this.traces) {
			while (this.traces.size() >= this.capacity) {
				this.traces.remove(this.reverse ? this.capacity - 1 : 0);
			}
			if (this.reverse) {
				this.traces.add(0, trace);
			}
			else {
				this.traces.add(trace);
			}
		}
	}

}
