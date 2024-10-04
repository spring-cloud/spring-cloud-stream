/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.cloud.stream.binder.Binding;

/**
 * A {@link Bindable} that stores the dynamic destination names and handles their
 * unbinding.
 *
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @author Omer Celik
 */
public final class DynamicDestinationsBindable implements Bindable {

	/**
	 * Map containing dynamic channel names and their bindings.
	 */
	private final Map<String, Binding<?>> outputBindings = new HashMap<>();

	private static final ReentrantLock lock = new ReentrantLock();

	public void addOutputBinding(String name, Binding<?> binding) {
		try {
			lock.lock();
			this.outputBindings.put(name, binding);
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public Set<String> getOutputs() {
		try {
			lock.lock();
			return Collections.unmodifiableSet(this.outputBindings.keySet());
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public void unbindOutputs(BindingService adapter) {
		try {
			lock.lock();
			for (Map.Entry<String, Binding<?>> entry : this.outputBindings.entrySet()) {
				entry.getValue().unbind();
			}
			this.outputBindings.clear();
		}
		finally {
			lock.unlock();
		}
	}

}
