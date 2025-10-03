/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.cloud.stream.binding.BindingTargetFactory} for
 * {@link KTable}
 *
 * Input bindings are only created as output bindings on KTable are not allowed.
 *
 * @author Soby Chacko
 */
public class KTableBoundElementFactory extends AbstractBindingTargetFactory<KTable> {

	private final BindingServiceProperties bindingServiceProperties;
	private final EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler;
	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	KTableBoundElementFactory(BindingServiceProperties bindingServiceProperties,
							EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler,
							KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue) {
		super(KTable.class);
		this.bindingServiceProperties = bindingServiceProperties;
		this.encodingDecodingBindAdviceHandler = encodingDecodingBindAdviceHandler;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
	}

	@Override
	public KTable createInput(String name) {
		BindingProperties bindingProperties = this.bindingServiceProperties.getBindingProperties(name);
		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if (consumerProperties == null) {
			consumerProperties = this.bindingServiceProperties.getConsumerProperties(name);
			consumerProperties.setUseNativeDecoding(true);
		}
		else {
			if (!encodingDecodingBindAdviceHandler.isDecodingSettingProvided()) {
				consumerProperties.setUseNativeDecoding(true);
			}
		}
		// Always set multiplex to true in the kafka streams binder
		consumerProperties.setMultiplex(true);

		KTableBoundElementFactory.KTableWrapperHandler wrapper = new KTableBoundElementFactory.KTableWrapperHandler();
		ProxyFactory proxyFactory = new ProxyFactory(
				KTableBoundElementFactory.KTableWrapper.class, KTable.class);
		proxyFactory.addAdvice(wrapper);

		final KTable proxy = (KTable) proxyFactory.getProxy();
		this.kafkaStreamsBindingInformationCatalogue.addBindingNamePerTarget(proxy, name);
		return proxy;
	}

	@Override
	@SuppressWarnings("unchecked")
	public KTable createOutput(final String name) {
		throw new UnsupportedOperationException(
				"Outbound operations are not allowed on target type KTable");
	}

	/**
	 * Wrapper for KTable proxy.
	 */
	public interface KTableWrapper {

		void wrap(KTable<Object, Object> delegate);

	}

	private static class KTableWrapperHandler
			implements KTableBoundElementFactory.KTableWrapper, MethodInterceptor {

		private KTable<Object, Object> delegate;

		public void wrap(KTable<Object, Object> delegate) {
			Assert.notNull(delegate, "delegate cannot be null");
			if (this.delegate == null) {
				this.delegate = delegate;
			}
		}

		@Override
		public Object invoke(MethodInvocation methodInvocation) throws Throwable {
			if (methodInvocation.getMethod().getDeclaringClass().equals(KTable.class)) {
				Assert.notNull(this.delegate,
						"Trying to prepareConsumerBinding " + methodInvocation.getMethod()
								+ "  but no delegate has been set.");
				return methodInvocation.getMethod().invoke(this.delegate,
						methodInvocation.getArguments());
			}
			else if (methodInvocation.getMethod().getDeclaringClass()
					.equals(KTableBoundElementFactory.KTableWrapper.class)) {
				return methodInvocation.getMethod().invoke(this,
						methodInvocation.getArguments());
			}
			else {
				throw new IllegalStateException(
						"Only KTable method invocations are permitted");
			}
		}

	}

}
