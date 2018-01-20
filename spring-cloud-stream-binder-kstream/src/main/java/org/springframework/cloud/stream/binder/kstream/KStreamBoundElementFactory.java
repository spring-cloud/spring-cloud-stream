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

package org.springframework.cloud.stream.binder.kstream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamBoundElementFactory extends AbstractBindingTargetFactory<KStream> implements ApplicationContextAware {

	private final BindingServiceProperties bindingServiceProperties;

	private final KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	private volatile AbstractApplicationContext applicationContext;

	private KStreamExtendedBindingProperties kStreamExtendedBindingProperties = new KStreamExtendedBindingProperties();

	public KStreamBoundElementFactory(BindingServiceProperties bindingServiceProperties,
									KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue,
									KeyValueSerdeResolver keyValueSerdeResolver) {
		super(KStream.class);
		this.bindingServiceProperties = bindingServiceProperties;
		this.KStreamBindingInformationCatalogue = KStreamBindingInformationCatalogue;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
	}

	public void setkStreamExtendedBindingProperties(KStreamExtendedBindingProperties kStreamExtendedBindingProperties) {
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isInstanceOf(AbstractApplicationContext.class, applicationContext);
		this.applicationContext = (AbstractApplicationContext) applicationContext;
	}

	@Override
	public KStream createInput(String name) {

		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
		String destination = bindingProperties.getDestination();
		if (destination == null) {
			destination = name;
		}
		KStreamConsumerProperties extendedConsumerProperties = kStreamExtendedBindingProperties.getExtendedConsumerProperties(name);
		Serde<?> keySerde = this.keyValueSerdeResolver.getInboundKeySerde(extendedConsumerProperties);

		Serde<?> valueSerde = this.keyValueSerdeResolver.getInboundValueSerde(bindingProperties.getConsumer(),
				extendedConsumerProperties);

		ConfigurableListableBeanFactory beanFactory = this.applicationContext.getBeanFactory();
		StreamsBuilderFactoryBean streamsBuilder = new StreamsBuilderFactoryBean();
		streamsBuilder.setAutoStartup(false);
		beanFactory.registerSingleton("stream-builder-" + destination, streamsBuilder);
		beanFactory.initializeBean(streamsBuilder, "stream-builder-" + destination);

		StreamsBuilder streamBuilder = null;
		try {
			streamBuilder = streamsBuilder.getObject();
		} catch (Exception e) {
			//log and bail
		}

		KStream<?,?> stream = streamBuilder.stream(bindingServiceProperties.getBindingDestination(name),
						Consumed.with(keySerde, valueSerde));
		stream = stream.map((key, value) -> {
			KeyValue<Object, Object> keyValue;
			String contentType = bindingProperties.getContentType();
			if (!StringUtils.isEmpty(contentType) && !bindingProperties.getConsumer().isUseNativeDecoding()) {
				Message<?> message = MessageBuilder.withPayload(value)
						.setHeader(MessageHeaders.CONTENT_TYPE, contentType).build();
				keyValue = new KeyValue<>(key, message);
			}
			else {
				keyValue = new KeyValue<>(key, value);
			}
			return keyValue;
		});
		this.KStreamBindingInformationCatalogue.registerBindingProperties(stream, bindingProperties);
		return stream;
	}

	@Override
	@SuppressWarnings("unchecked")
	public KStream createOutput(final String name) {
		KStreamWrapperHandler wrapper= new KStreamWrapperHandler();
		ProxyFactory proxyFactory = new ProxyFactory(KStreamWrapper.class, KStream.class);
		proxyFactory.addAdvice(wrapper);

		KStream proxy = (KStream) proxyFactory.getProxy();

		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
		this.KStreamBindingInformationCatalogue.registerBindingProperties(proxy, bindingProperties);
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
