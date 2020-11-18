/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.stub2;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;

/**
 * @author Marius Bogoevici
 * @author Mark Fisher
 */
public class StubBinder2
		implements Binder<Object, ConsumerProperties, ProducerProperties> {

	@SuppressWarnings("unused")
	private final StubBinder2Dependency stubBinder2Dependency;

	private String fromCustomization;

	public StubBinder2(StubBinder2Dependency stubBinder2Dependency) {
		this.stubBinder2Dependency = stubBinder2Dependency;
	}

	@Override
	public Binding<Object> bindConsumer(String name, String group,
			Object inboundBindTarget, ConsumerProperties properties) {
		return null;
	}

	@Override
	public Binding<Object> bindProducer(String name, Object outboundBindTarget,
			ProducerProperties properties) {
		return null;
	}

	public String getFromCustomization() {
		return fromCustomization;
	}

	public void setFromCustomization(String fromCustomization) {
		this.fromCustomization = fromCustomization;
	}
}
