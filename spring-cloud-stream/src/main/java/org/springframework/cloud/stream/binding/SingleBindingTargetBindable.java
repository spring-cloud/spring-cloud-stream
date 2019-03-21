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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.cloud.stream.binder.Binding;

/**
 * A {@link Bindable} component that wraps a generic output binding target. Useful for
 * binding targets outside the {@link org.springframework.cloud.stream.annotation.Input}
 * and {@link org.springframework.cloud.stream.annotation.Output} annotated interfaces.
 *
 * @param <T> type of binding target
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @deprecated This class is no longer used by the framework and maybe removed in a future
 * release.
 */
@Deprecated
public class SingleBindingTargetBindable<T> implements Bindable {

	private final String name;

	private final T bindingTarget;

	public SingleBindingTargetBindable(String name, T bindingTarget) {
		this.name = name;
		this.bindingTarget = bindingTarget;
	}

	@Override
	public void bindOutputs(BindingService bindingService) {
		this.createAndBindOutputs(bindingService);
	}

	@Override
	public Collection<Binding<Object>> createAndBindOutputs(
			BindingService bindingService) {
		return Collections.singletonList(
				bindingService.bindProducer(this.bindingTarget, this.name));
	}

	@Override
	public void unbindOutputs(BindingService bindingService) {
		bindingService.unbindProducers(this.name);
	}

	@Override
	public Set<String> getOutputs() {
		return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(this.name)));
	}

}
