/*
 * Copyright 2018-2020 the original author or authors.
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
import org.apache.kafka.streams.kstream.GlobalKTable;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.cloud.stream.binding.BindingTargetFactory} for
 * {@link GlobalKTable}
 *
 * Input bindings are only created as output bindings on GlobalKTable are not allowed.
 *
 * @author Soby Chacko
 * @since 2.1.0
 */
public class GlobalKTableBoundElementFactory
		extends AbstractBindingTargetFactory<GlobalKTable> {

	private final BindingServiceProperties bindingServiceProperties;
	private final EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler;
	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	GlobalKTableBoundElementFactory(BindingServiceProperties bindingServiceProperties,
									EncodingDecodingBindAdviceHandler encodingDecodingBindAdviceHandler,
									KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue) {
		super(GlobalKTable.class);
		this.bindingServiceProperties = bindingServiceProperties;
		this.encodingDecodingBindAdviceHandler = encodingDecodingBindAdviceHandler;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
	}

	@Override
	public GlobalKTable createInput(String name) {
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

		// @checkstyle:off
		GlobalKTableBoundElementFactory.GlobalKTableWrapperHandler wrapper = new GlobalKTableBoundElementFactory.GlobalKTableWrapperHandler();
		// @checkstyle:on
		ProxyFactory proxyFactory = new ProxyFactory(
				GlobalKTableBoundElementFactory.GlobalKTableWrapper.class,
				GlobalKTable.class);
		proxyFactory.addAdvice(wrapper);

		final GlobalKTable proxy = (GlobalKTable) proxyFactory.getProxy();
		this.kafkaStreamsBindingInformationCatalogue.addBindingNamePerTarget(proxy, name);
		return proxy;
	}

	@Override
	public GlobalKTable createOutput(String name) {
		throw new UnsupportedOperationException(
				"Outbound operations are not allowed on target type GlobalKTable");
	}

	/**
	 * Wrapper for GlobalKTable proxy.
	 */
	public interface GlobalKTableWrapper {

		void wrap(GlobalKTable<Object, Object> delegate);

	}

	private static class GlobalKTableWrapperHandler implements
			GlobalKTableBoundElementFactory.GlobalKTableWrapper, MethodInterceptor {

		private GlobalKTable<Object, Object> delegate;

		public void wrap(GlobalKTable<Object, Object> delegate) {
			Assert.notNull(delegate, "delegate cannot be null");
			if (this.delegate == null) {
				this.delegate = delegate;
			}
		}

		@Override
		public Object invoke(MethodInvocation methodInvocation) throws Throwable {
			if (methodInvocation.getMethod().getDeclaringClass()
					.equals(GlobalKTable.class)) {
				Assert.notNull(this.delegate,
						"Trying to prepareConsumerBinding " + methodInvocation.getMethod()
								+ "  but no delegate has been set.");
				return methodInvocation.getMethod().invoke(this.delegate,
						methodInvocation.getArguments());
			}
			else if (methodInvocation.getMethod().getDeclaringClass()
					.equals(GlobalKTableBoundElementFactory.GlobalKTableWrapper.class)) {
				return methodInvocation.getMethod().invoke(this,
						methodInvocation.getArguments());
			}
			else {
				throw new IllegalStateException(
						"Only GlobalKTable method invocations are permitted");
			}
		}

	}

}
