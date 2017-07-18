/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.lang.reflect.Method;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.annotation.ErrorListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.ReflectionUtils;

/**
 * A {@link BeanPostProcessor} that handles {@link ErrorListener} annotations found on bean methods.
 *
 * @author Ilayaperumal Gopinathan
 */
public class ErrorListenerAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

	@Autowired
	@Lazy
	private DestinationResolver<MessageChannel> binderAwareChannelResolver;

	@Autowired
	@Lazy
	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private ConfigurableApplicationContext applicationContext;

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public final Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public final Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
		ReflectionUtils.doWithMethods(targetClass, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(final Method method) throws IllegalArgumentException, IllegalAccessException {
				ErrorListener errorListener = AnnotatedElementUtils.findMergedAnnotation(method,
						ErrorListener.class);
				if (errorListener != null && !method.isBridge()) {
					registerHandlerMethodOnListenedChannel(method, errorListener, bean);
				}
			}
		});
		return bean;
	}

	protected final void registerHandlerMethodOnListenedChannel(Method method, ErrorListener errorListener,
			Object bean) {
		Method targetMethod = StreamListenerMethodUtils.checkProxy(method, bean);
		final InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(bean, targetMethod);
		PublishSubscribeChannel channel = this.applicationContext.getBean(
				IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, PublishSubscribeChannel.class);
		ListenerMethodMessageHandler handler = new ListenerMethodMessageHandler(invocableHandlerMethod);
		handler.setApplicationContext(this.applicationContext);
		handler.setChannelResolver(this.binderAwareChannelResolver);
		handler.afterPropertiesSet();
		channel.subscribe(handler);
	}

}
