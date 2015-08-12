/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import java.util.Properties;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.messaging.MessageChannel;

/**
 * Created by ericbottard on 12/08/15.
 */
public class TestSupportBinder implements Binder<MessageChannel> {
	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {

	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {

	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {

	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {

	}

	@Override
	public void unbindConsumers(String name) {

	}

	@Override
	public void unbindProducers(String name) {

	}

	@Override
	public void unbindConsumer(String name, MessageChannel inboundBindTarget) {

	}

	@Override
	public void unbindProducer(String name, MessageChannel outboundBindTarget) {

	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {

	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {

	}

	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		return null;
	}

	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		return null;
	}

	@Override
	public boolean isCapable(Capability capability) {
		return false;
	}
}
