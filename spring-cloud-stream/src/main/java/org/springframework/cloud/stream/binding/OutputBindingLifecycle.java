/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.util.CollectionUtils;

/**
 * Coordinates binding/unbinding of output binding targets in accordance to the lifecycle
 * of the host context.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
public class OutputBindingLifecycle extends AbstractBindingLifecycle {

	@SuppressWarnings("unused")
	// It is actually used reflectively since at the moment we do not want to expose it
	// via public method
	private Collection<Binding<Object>> outputBindings = new ArrayList<Binding<Object>>();

	public OutputBindingLifecycle(BindingService bindingService,
			Map<String, Bindable> bindables) {
		super(bindingService, bindables);
	}

	/**
	 * Return a low value so that this bean is started after receiving Lifecycle beans are
	 * started. Beans that need to start before bindings will set a lower phase value.
	 */
	@Override
	public int getPhase() {
		return Integer.MIN_VALUE + 1000;
	}

	@Override
	void doStartWithBindable(Bindable bindable) {
		Collection<Binding<Object>> bindableBindings = bindable
				.createAndBindOutputs(this.bindingService);
		if (!CollectionUtils.isEmpty(bindableBindings)) {
			this.outputBindings.addAll(bindableBindings);
		}
	}

	@Override
	void doStopWithBindable(Bindable bindable) {
		bindable.unbindOutputs(this.bindingService);
	}

}
