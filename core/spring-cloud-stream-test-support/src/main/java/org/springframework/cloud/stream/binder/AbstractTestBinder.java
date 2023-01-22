/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.HashSet;
import java.util.Set;

import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Abstract class that adds test support for {@link Binder}.
 *
 * @param <C> binder type
 * @param <CP> consumer properties type
 * @param <PP> producer properties type
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 */
// @checkstyle:off
public abstract class AbstractTestBinder<C extends AbstractBinder<MessageChannel, CP, PP>, CP extends ConsumerProperties, PP extends ProducerProperties>
		implements Binder<MessageChannel, CP, PP> {

	// @checkstyle:on

	protected Set<String> queues = new HashSet<String>();

	private C binder;

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group,
			MessageChannel moduleInputChannel, CP properties) {
		this.checkChannelIsConfigured(moduleInputChannel, properties);
		this.queues.add(name);
		return this.binder.bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name,
			MessageChannel moduleOutputChannel, PP properties) {
		this.queues.add(name);
		return this.binder.bindProducer(name, moduleOutputChannel, properties);
	}

	public C getCoreBinder() {
		return this.binder;
	}

	public abstract void cleanup();

	public C getBinder() {
		return this.binder;
	}

	public void setBinder(C binder) {
		try {
			binder.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to initialize binder", e);
		}
		this.binder = binder;
	}

	/*
	 * This will ensure that any MessageChannel that was passed to one of the bind*()
	 * methods was properly configured (i.e., interceptors, converters etc). see
	 * org.springframework.cloud.stream.binding.MessageConverterConfigurer
	 */
	private void checkChannelIsConfigured(MessageChannel messageChannel, CP properties) {
		if (messageChannel instanceof AbstractSubscribableChannel subscribableMessageChannel
				&& !properties.isUseNativeDecoding()) {
			Assert.isTrue(
					!CollectionUtils
							.isEmpty(subscribableMessageChannel.getInterceptors()),
					"'messageChannel' appears to be misconfigured. "
							+ "Consider creating channel via AbstractBinderTest.createBindableChannel(..)");
		}
	}

}
