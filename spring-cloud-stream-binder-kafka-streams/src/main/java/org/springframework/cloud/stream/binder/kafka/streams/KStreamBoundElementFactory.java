/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.cloud.stream.binding.BindingTargetFactory} for{@link KStream}.
 *
 * The implementation creates proxies for both input and output binding.
 * The actual target will be created downstream through further binding process.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
class KStreamBoundElementFactory extends AbstractBindingTargetFactory<KStream> {

	private final BindingServiceProperties bindingServiceProperties;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	KStreamBoundElementFactory(BindingServiceProperties bindingServiceProperties,
									KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue) {
		super(KStream.class);
		this.bindingServiceProperties = bindingServiceProperties;
		this.kafkaStreamsBindingInformationCatalogue = KafkaStreamsBindingInformationCatalogue;
	}

	@Override
	public KStream createInput(String name) {
		ConsumerProperties consumerProperties = this.bindingServiceProperties.getConsumerProperties(name);
		//Always set multiplex to true in the kafka streams binder
		consumerProperties.setMultiplex(true);
		return createProxyForKStream(name);
	}

	@Override
	@SuppressWarnings("unchecked")
	public KStream createOutput(final String name) {
		return createProxyForKStream(name);
	}

	private KStream createProxyForKStream(String name) {
		KStreamWrapperHandler wrapper= new KStreamWrapperHandler();
		ProxyFactory proxyFactory = new ProxyFactory(KStreamWrapper.class, KStream.class);
		proxyFactory.addAdvice(wrapper);

		KStream proxy = (KStream) proxyFactory.getProxy();

		//Add the binding properties to the catalogue for later retrieval during further binding steps downstream.
		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
		this.kafkaStreamsBindingInformationCatalogue.registerBindingProperties(proxy, bindingProperties);
		return proxy;
	}

	public interface KStreamWrapper {

		void wrap(KStream<Object, Object> delegate);

	}

	private static class KStreamWrapperHandler implements KStreamWrapper, MethodInterceptor {

		private KStream<Object, Object> delegate;

		public void wrap(KStream<Object, Object> delegate) {
			Assert.notNull(delegate, "delegate cannot be null");
			Assert.isNull(this.delegate, "delegate already set to " + this.delegate);
			this.delegate = delegate;
		}

		@Override
		public Object invoke(MethodInvocation methodInvocation) throws Throwable {
			if (methodInvocation.getMethod().getDeclaringClass().equals(KStream.class)) {
				Assert.notNull(delegate, "Trying to invoke " + methodInvocation
						.getMethod() + "  but no delegate has been set.");
				return methodInvocation.getMethod().invoke(delegate, methodInvocation.getArguments());
			}
			else if (methodInvocation.getMethod().getDeclaringClass().equals(KStreamWrapper.class)) {
				return methodInvocation.getMethod().invoke(this, methodInvocation.getArguments());
			}
			else {
				throw new IllegalStateException("Only KStream method invocations are permitted");
			}
		}
	}
}
