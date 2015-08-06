/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binding;

import java.util.Map;

import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Exposes a context as a {@link Bindable} component, allowing for embedding {@link EnableModule}
 * units as child contexts into an application.
 *
 * @author Marius Bogoevici
 */
public class BindableContextWrapper implements Bindable {

	private ConfigurableApplicationContext context;

	public BindableContextWrapper(ConfigurableApplicationContext context) {
		this.context = context;
	}

	public ConfigurableApplicationContext getContext() {
		return context;
	}

	@Override
	public void bindInputs(ChannelBindingAdapter channelBindingAdapter) {
		if (context.isActive()) {
			Map<String, Bindable> bindables = context.getBeansOfType(Bindable.class);
			for (Bindable bindable : bindables.values()) {
				bindable.bindInputs(channelBindingAdapter);
			}
		}
	}

	@Override
	public void bindOutputs(ChannelBindingAdapter channelBindingAdapter) {
		if (context.isActive()) {
			Map<String, Bindable> bindables = context.getBeansOfType(Bindable.class);
			for (Bindable bindable : bindables.values()) {
				bindable.bindOutputs(channelBindingAdapter);
			}
		}
	}

	@Override
	public void unbindInputs(ChannelBindingAdapter channelBindingAdapter) {
		if (context.isActive()) {
			Map<String, Bindable> bindables = context.getBeansOfType(Bindable.class);
			for (Bindable bindable : bindables.values()) {
				bindable.unbindInputs(channelBindingAdapter);
			}
		}
	}

	@Override
	public void unbindOutputs(ChannelBindingAdapter channelBindingAdapter) {
		if (context.isActive()) {
			Map<String, Bindable> bindables = context.getBeansOfType(Bindable.class);
			for (Bindable bindable : bindables.values()) {
				bindable.unbindOutputs(channelBindingAdapter);
			}
		}
	}
}
