/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.stub1;

import java.util.Properties;

import org.springframework.cloud.stream.binder.Binder;

/**
 * @author Marius Bogoevici
 */
public class StubBinder1 implements Binder<Object> {

	private String name;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public void bindConsumer(String name, String group, Object inboundBindTarget, Properties properties) {

	}

	@Override
	public void bindProducer(String name, Object outboundBindTarget, Properties properties) {

	}

	@Override
	public void unbindConsumers(String name, String group) {

	}

	@Override
	public void unbindProducers(String name) {

	}

	@Override
	public void unbindConsumer(String name, String group, Object inboundBindTarget) {

	}

	@Override
	public void unbindProducer(String name, Object outboundBindTarget) {

	}

}
