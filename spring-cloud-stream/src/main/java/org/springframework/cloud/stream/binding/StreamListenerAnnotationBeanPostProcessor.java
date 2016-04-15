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
import java.util.HashMap;
import java.util.Map;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.annotation.StreamListener;
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
 * {@link BeanPostProcessor} that handles {@link StreamListener} annotations found on bean methods.
 *
 * @author Marius Bogoevici
 */
public class StreamListenerAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton {

	private final DestinationResolver<MessageChannel> binderAwareChannelResolver;

	private final MessageHandlerMethodFactory messageHandlerMethodFactory;

	private final Map<String, InvocableHandlerMethod> mappedBindings = new HashMap<>();

	private ConfigurableApplicationContext applicationContext;

	public StreamListenerAnnotationBeanPostProcessor(DestinationResolver<MessageChannel> binderAwareChannelResolver, MessageHandlerMethodFactory messageHandlerMethodFactory) {
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
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
		ReflectionUtils.doWithMethods(targetClass, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(final Method method) throws IllegalArgumentException, IllegalAccessException {
				StreamListener streamListener = AnnotationUtils.findAnnotation(method, StreamListener.class);
				if (streamListener != null) {
					Method targetMethod = checkProxy(method, bean);
					Assert.hasText(streamListener.value(), "The binding name cannot be null");
					final InvocableHandlerMethod invocableHandlerMethod = messageHandlerMethodFactory.createInvocableHandlerMethod(bean, targetMethod);
					if (!StringUtils.hasText(streamListener.value())) {
						throw new BeanInitializationException("A bound component name must be specified");
					}
					if (mappedBindings.containsKey(streamListener.value())) {
						throw new BeanInitializationException("Duplicate @" + StreamListener.class.getSimpleName() +
								" mapping for '" + streamListener.value() + "' on " + invocableHandlerMethod.getShortLogMessage() +
								" already existing for " + mappedBindings.get(streamListener.value()).getShortLogMessage());
					}
					mappedBindings.put(streamListener.value(), invocableHandlerMethod);
					SubscribableChannel channel = applicationContext.getBean(streamListener.value(),
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
					StreamListenerMessageHandler handler = new StreamListenerMessageHandler(invocableHandlerMethod);
					handler.setApplicationContext(applicationContext);
					handler.setChannelResolver(binderAwareChannelResolver);
					if (!StringUtils.isEmpty(defaultOutputChannel)) {
						handler.setOutputChannelName(defaultOutputChannel);
					}
					handler.afterPropertiesSet();
					channel.subscribe(handler);
				}
			}
		});
		return bean;
	}

	@Override
	public void afterSingletonsInstantiated() {
		// Dump the mappings after the context has been created, ensuring that beans can be processed correctly
		// again.
		mappedBindings.clear();
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

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @StreamListener method on the target class for this JDK proxy ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					}
					catch (NoSuchMethodException noMethod) {
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@StreamListener method '%s' found on bean target class '%s', " +
								"but not found in any interface(s) for bean JDK proxy. Either " +
								"pull the method up to an interface or switch to subclass (CGLIB) " +
								"proxies by setting proxy-target-class/proxyTargetClass " +
								"attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	private class StreamListenerMessageHandler extends AbstractReplyProducingMessageHandler {

		private final InvocableHandlerMethod invocableHandlerMethod;

		public StreamListenerMessageHandler(InvocableHandlerMethod invocableHandlerMethod) {
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
					throw new MessagingException(requestMessage, "Exception thrown while invoking " + invocableHandlerMethod.getShortLogMessage(), e);
				}
			}
		}
	}
}
