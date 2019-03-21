/*
 * Copyright 2016-2019 the original author or authors.
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

import org.springframework.cloud.stream.binder.Binding;

/**
 * A {@link Bindable} that stores the dynamic destination names and handles their
 * unbinding.
 *
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public final class DynamicDestinationsBindable implements Bindable {

	/**
	 * Map containing dynamic channel names and their bindings.
	 */
	private final Map<String, Binding<?>> outputBindings = new HashMap<>();

	public synchronized void addOutputBinding(String name, Binding<?> binding) {
		this.outputBindings.put(name, binding);
	}

	@Override
	public synchronized Set<String> getOutputs() {
		return Collections.unmodifiableSet(this.outputBindings.keySet());
	}

	@Override
	public synchronized void unbindOutputs(BindingService adapter) {
		for (Map.Entry<String, Binding<?>> entry : this.outputBindings.entrySet()) {
			entry.getValue().unbind();
		}
		this.outputBindings.clear();
	}

}
