/*
 * Copyright 2015-2019 the original author or authors.
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

/**
 * Holds information about the binding targets exposed by the interface proxy, as well
 * as their status.
 *
 * Refactored from {@link BindableProxyFactory}.
 *
 * @author Original authors in {@link BindableProxyFactory}
 * @author Soby Chacko
 * @since 3.0.0
 */
public final class BoundTargetHolder {

	private Object boundTarget;

	private boolean bindable;

	public BoundTargetHolder(Object boundTarget, boolean bindable) {
		this.boundTarget = boundTarget;
		this.bindable = bindable;
	}

	public Object getBoundTarget() {
		return this.boundTarget;
	}

	public boolean isBindable() {
		return this.bindable;
	}

}
