/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.annotation.BindingListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
public class BindingListenerAnnotationBeanPostProcessor implements BeanPostProcessor, SmartInitializingSingleton, ApplicationContextAware {

	private final DestinationResolver<MessageChannel> binderAwareChannelResolver;

	private final MessageHandlerMethodFactory messageHandlerMethodFactory;

	private ConfigurableApplicationContext applicationContext;

	public BindingListenerAnnotationBeanPostProcessor(DestinationResolver<MessageChannel> binderAwareChannelResolver, MessageHandlerMethodFactory messageHandlerMethodFactory) {
		Assert.notNull(binderAwareChannelResolver, "Destination resolver cannot be null");
		Assert.notNull(messageHandlerMethodFactory, "Message handler method factory cannot be null");
		this.binderAwareChannelResolver = binderAwareChannelResolver;
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, String beanName) throws BeansException {
		ReflectionUtils.doWithMethods(bean.getClass(), new ReflectionUtils.MethodCallback() {

			@Override
			public void doWith(final Method method) throws IllegalArgumentException, IllegalAccessException {
				BindingListener bindingListener = AnnotationUtils.findAnnotation(method, BindingListener.class);
				if (bindingListener != null) {
					Assert.hasText(bindingListener.value(), "The binding name cannot be null");
					final InvocableHandlerMethod invocableHandlerMethod = messageHandlerMethodFactory.createInvocableHandlerMethod(bean, method);
					if (!StringUtils.hasText(bindingListener.value())) {
						throw new BeanInitializationException("Only one of 'bindings' or 'value' can be specified");
					}
					SubscribableChannel channel = applicationContext.getBean(bindingListener.value(),
							SubscribableChannel.class);
					final String defaultOutputChannel = extractDefaultOutput(method);
					if (invocableHandlerMethod.isVoid()) {
						Assert.isTrue(StringUtils.isEmpty(defaultOutputChannel), "An output channel cannot be specified for a method that " +
																						 "does not return a value");
					}
					else {
						Assert.isTrue(!StringUtils.isEmpty(defaultOutputChannel), "An output channel must be specified for a method that " +
																						  "can return a value");
					}
					BindingMessageHandler handler = new BindingMessageHandler(invocableHandlerMethod);
					handler.setApplicationContext(applicationContext);
					handler.setChannelResolver(binderAwareChannelResolver);
					if (!StringUtils.isEmpty(defaultOutputChannel)) {
						handler.setOutputChannelName(defaultOutputChannel);
					}
					handler.afterPropertiesSet();
					// // TODO: add support for pollable channels
					channel.subscribe(handler);
				}
			}
		});
		return bean;
	}

	private String extractDefaultOutput(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), "At least one output must be specified");
			Assert.isTrue(sendTo.value().length == 1, "Multiple destinations cannot be specified");
			Assert.hasText(sendTo.value()[0], "An empty destination cannot be specified");
			return sendTo.value()[0];
		}
		return null;
	}

	@Override
	public void afterSingletonsInstantiated() {

	}

	private class BindingMessageHandler extends AbstractReplyProducingMessageHandler {

		private final InvocableHandlerMethod invocableHandlerMethod;

		public BindingMessageHandler(InvocableHandlerMethod invocableHandlerMethod) {
			this.invocableHandlerMethod = invocableHandlerMethod;
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			return false;
		}

		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			try {
				return invocableHandlerMethod.invoke(requestMessage);
			}
			catch (Exception e) {
				if (e instanceof MessagingException) {
					throw (MessagingException) e;
				}
				else {
					throw new MessagingException(requestMessage,
														"Exception thrown while invoking " + invocableHandlerMethod.getShortLogMessage(),
														e);
				}
			}
		}
	}
}
