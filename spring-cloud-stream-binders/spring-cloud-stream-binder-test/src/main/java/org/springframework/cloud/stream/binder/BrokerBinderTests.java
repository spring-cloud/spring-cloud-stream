/*
 * Copyright 2014-2016 the original author or authors.
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

import org.springframework.messaging.MessageChannel;

/**
 * Tests for binders that use an external broker.
 *
 * @author Gary Russell
 */
public abstract class BrokerBinderTests<B extends AbstractTestBinder<? extends AbstractBinder<MessageChannel, CP, PP>, CP, PP>, CP extends ConsumerProperties, PP extends ProducerProperties>
		extends AbstractBinderTests<B, CP, PP> {

	/**
	 * Create a new spy on the given 'queue'. This allows de-correlating the creation of
	 * the 'connection' from its actual usage, which may be needed by some implementations
	 * to see messages sent after connection creation.
	 */
	public abstract Spy spyOn(final String name);

}
