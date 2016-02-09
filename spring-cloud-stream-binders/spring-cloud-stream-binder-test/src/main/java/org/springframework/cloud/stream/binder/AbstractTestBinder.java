/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.messaging.MessageChannel;

/**
 * Abstract class that adds test support for {@link Binder}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Mark Fisher
 */
public abstract class AbstractTestBinder<C extends AbstractBinder> implements Binder<MessageChannel> {

	protected Set<String> queues = new HashSet<String>();

	private C binder;

	public void setBinder(C binder) {
		try {
			binder.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to initialize binder", e);
		}
		this.binder = binder;
	}

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel moduleInputChannel, Properties properties) {
		queues.add(name);
		return binder.bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel, Properties properties) {
		queues.add(name);
		return binder.bindProducer(name, moduleOutputChannel, properties); 
	}

	public C getCoreBinder() {
		return binder;
	}

	public abstract void cleanup();

	public C getBinder() {
		return this.binder;
	}

}
