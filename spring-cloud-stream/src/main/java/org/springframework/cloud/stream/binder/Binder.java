/*
 * Copyright 2013-2016 the original author or authors.
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

import java.util.Properties;

/**
 * A strategy interface used to bind an app interface to a logical name. The name is intended to identify a
 * logical consumer or producer of messages. This may be a queue, a channel adapter, another message channel, a Spring
 * bean, etc.
 *
 * @author Mark Fisher
 * @author David Turanski
 * @author Gary Russell
 * @author Jennifer Hickey
 * @author Ilayaperumal Gopinathan
 * @since 1.0
 */
public interface Binder<T> {

	/**
	 * Bind the target component as a message consumer to the logical entity identified by the name.
	 * @param name the logical identity of the message source
	 * @param group the consumer group to which this consumer belongs - subscriptions are shared among consumers
	 * in the same group (a <code>null</code> or empty String, must be treated as an anonymous group that doesn't share
	 * the subscription with any other consumer)
	 * @param inboundBindTarget the app interface to be bound as a consumer
	 * @param properties arbitrary String key/value pairs that will be used as consumer properties in the binding
	 */
	Binding<T> bindConsumer(String name, String group, T inboundBindTarget, Properties properties);

	/**
	 * Bind the target component as a message producer to the logical entity identified by the name.
	 * @param name the logical identity of the message target
	 * @param outboundBindTarget the app interface to be bound as a producer
	 * @param properties arbitrary String key/value pairs that will be used as producer properties in the binding
	 */
	Binding<T> bindProducer(String name, T outboundBindTarget, Properties properties);

}
