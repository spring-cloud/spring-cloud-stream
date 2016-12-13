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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link BeanPostProcessor} that handles {@link StreamListener} annotations found on bean
 * methods.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton {

	private final DestinationResolver<MessageChannel> binderAwareChannelResolver;

	private final MessageHandlerMethodFactory messageHandlerMethodFactory;

	private final Map<String, InvocableHandlerMethod> mappedBindings = new HashMap<>();

	private ConfigurableApplicationContext applicationContext;

	private final List<StreamListenerParameterAdapter<?, Object>> streamListenerParameterAdapters = new ArrayList<>();

	private final List<StreamListenerResultAdapter<?, ?>> streamListenerResultAdapters = new ArrayList<>();

	public StreamListenerAnnotationBeanPostProcessor(DestinationResolver<MessageChannel> binderAwareChannelResolver,
			MessageHandlerMethodFactory messageHandlerMethodFactory) {
		Assert.notNull(binderAwareChannelResolver, "Destination resolver cannot be null");
		Assert.notNull(messageHandlerMethodFactory, "Message handler method factory cannot be null");
		this.binderAwareChannelResolver = binderAwareChannelResolver;
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		Map<String, StreamListenerParameterAdapter> parameterAdapterMap = this.applicationContext
				.getBeansOfType(StreamListenerParameterAdapter.class);
		for (StreamListenerParameterAdapter parameterAdapter : parameterAdapterMap.values()) {
			this.streamListenerParameterAdapters.add(parameterAdapter);
		}
		Map<String, StreamListenerResultAdapter> resultAdapterMap = this.applicationContext
				.getBeansOfType(StreamListenerResultAdapter.class);
		this.streamListenerResultAdapters.add(new MessageChannelStreamListenerResultAdapter());
		for (StreamListenerResultAdapter resultAdapter : resultAdapterMap.values()) {
			this.streamListenerResultAdapters.add(resultAdapter);
		}
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
					Assert.isTrue(method.getAnnotation(Input.class) == null,
							StreamListenerErrorMessages.INPUT_AT_STREAM_LISTENER);
					String methodAnnotatedInboundName = streamListener.value();
					String methodAnnotatedOutboundName = StreamListenerMethodUtils.getOutboundBindingTargetName(method);
					int inputAnnotationCount = StreamListenerMethodUtils.inputAnnotationCount(method);
					int outputAnnotationCount = StreamListenerMethodUtils.outputAnnotationCount(method);
					boolean isDeclarative = checkDeclarativeMethod(method, methodAnnotatedInboundName,
							methodAnnotatedOutboundName);
					StreamListenerMethodUtils.validateStreamListenerMethod(method, inputAnnotationCount,
							outputAnnotationCount, methodAnnotatedInboundName, methodAnnotatedOutboundName,
							isDeclarative);
					if (!method.getReturnType().equals(Void.TYPE)) {
						if (!StringUtils.hasText(methodAnnotatedOutboundName)) {
							if (outputAnnotationCount == 0) {
								throw new IllegalArgumentException(
										StreamListenerErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
							}
							Assert.isTrue((outputAnnotationCount == 1),
									StreamListenerErrorMessages.RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED);
						}
					}
					if (isDeclarative) {
						invokeSetupMethodOnListenedChannel(method, bean, methodAnnotatedInboundName,
								methodAnnotatedOutboundName);
					}
					else {
						registerHandlerMethodOnListenedChannel(method, streamListener, bean);
					}
				}
			}
		});
		return bean;
	}

	private boolean checkDeclarativeMethod(Method method, String methodAnnotatedInboundName,
			String methodAnnotatedOutboundName) {
		int methodArgumentsLength = method.getParameterTypes().length;
		for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				String inboundName = (String) AnnotationUtils
						.getValue(methodParameter.getParameterAnnotation(Input.class));
				Assert.isTrue(StringUtils.hasText(inboundName), StreamListenerErrorMessages.INVALID_INBOUND_NAME);
				Assert.isTrue(
						isDeclarativeMethodParameter(this.applicationContext.getBean(inboundName), methodParameter),
						StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
				return true;
			}
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				String outboundName = (String) AnnotationUtils
						.getValue(methodParameter.getParameterAnnotation(Output.class));
				Assert.isTrue(StringUtils.hasText(outboundName), StreamListenerErrorMessages.INVALID_OUTBOUND_NAME);
				Assert.isTrue(
						isDeclarativeMethodParameter(this.applicationContext.getBean(outboundName), methodParameter),
						StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
				return true;
			}
			if (StringUtils.hasText(methodAnnotatedOutboundName)) {
				return isDeclarativeMethodParameter(this.applicationContext.getBean(methodAnnotatedOutboundName),
						methodParameter);
			}
			if (StringUtils.hasText(methodAnnotatedInboundName)) {
				return isDeclarativeMethodParameter(this.applicationContext.getBean(methodAnnotatedInboundName),
						methodParameter);
			}
		}
		return false;
	}

	private boolean isDeclarativeMethodParameter(Object targetBean, MethodParameter methodParameter) {
		if (targetBean != null) {
			if (methodParameter.getParameterType().isAssignableFrom(targetBean.getClass())) {
				return true;
			}
			for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : this.streamListenerParameterAdapters) {
				if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void invokeSetupMethodOnListenedChannel(Method method, Object bean, String inboundName,
			String outboundName) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
			}
			else if (methodParameter.hasParameterAnnotation(Output.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
			}
			else if (arguments.length == 1 && StringUtils.hasText(inboundName)) {
				targetReferenceValue = inboundName;
			}
			if (targetReferenceValue != null) {
				Assert.isInstanceOf(String.class, targetReferenceValue, "Annotation value must be a String");
				Object targetBean = this.applicationContext.getBean((String) targetReferenceValue);
				if (parameterType.isAssignableFrom(targetBean.getClass())) {
					arguments[parameterIndex] = targetBean;
				}
				else {
					for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : this.streamListenerParameterAdapters) {
						if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
							arguments[parameterIndex] = streamListenerParameterAdapter.adapt(targetBean,
									methodParameter);
							break;
						}
					}
				}
				Assert.notNull(arguments[parameterIndex], "Cannot convert argument " + parameterIndex + " of " + method
						+ "from " + targetBean.getClass() + " to " + parameterType);
			}
			else {
				throw new IllegalStateException(StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		try {
			if (method.getReturnType().equals(Void.TYPE)) {
				method.invoke(bean, arguments);
			}
			else {
				Object result = method.invoke(bean, arguments);
				if (!StringUtils.hasText(outboundName)) {
					for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
						MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method,
								parameterIndex);
						if (methodParameter.hasParameterAnnotation(Output.class)) {
							outboundName = methodParameter.getParameterAnnotation(Output.class).value();
						}
					}
				}
				Object targetBean = this.applicationContext.getBean(outboundName);
				for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
					if (streamListenerResultAdapter.supports(result.getClass(), targetBean.getClass())) {
						streamListenerResultAdapter.adapt(result, targetBean);
						break;
					}
				}
			}
		}
		catch (Exception e) {
			throw new BeanInitializationException("Cannot setup StreamListener for " + method, e);
		}
	}

	protected void registerHandlerMethodOnListenedChannel(Method method, StreamListener streamListener, Object bean) {
		Method targetMethod = checkProxy(method, bean);
		Assert.hasText(streamListener.value(), "The binding name cannot be null");
		final InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(bean, targetMethod);
		if (!StringUtils.hasText(streamListener.value())) {
			throw new BeanInitializationException("A bound component name must be specified");
		}
		if (this.mappedBindings.containsKey(streamListener.value())) {
			throw new BeanInitializationException("Duplicate @" + StreamListener.class.getSimpleName()
					+ " mapping for '" + streamListener.value() + "' on " + invocableHandlerMethod.getShortLogMessage()
					+ " already existing for " + this.mappedBindings.get(streamListener.value()).getShortLogMessage());
		}
		this.mappedBindings.put(streamListener.value(), invocableHandlerMethod);
		SubscribableChannel channel = this.applicationContext.getBean(streamListener.value(),
				SubscribableChannel.class);
		final String defaultOutputChannel = StreamListenerMethodUtils.getOutboundBindingTargetName(method);
		if (invocableHandlerMethod.isVoid()) {
			Assert.isTrue(StringUtils.isEmpty(defaultOutputChannel),
					"An output channel cannot be specified for a method that does not return a value");
		}
		else {
			Assert.isTrue(!StringUtils.isEmpty(defaultOutputChannel),
					"An output channel must be specified for a method that can return a value");
		}
		StreamListenerMethodUtils.validateStreamListenerMessageHandler(method);
		StreamListenerMessageHandler handler = new StreamListenerMessageHandler(invocableHandlerMethod);
		handler.setApplicationContext(this.applicationContext);
		handler.setChannelResolver(this.binderAwareChannelResolver);
		if (!StringUtils.isEmpty(defaultOutputChannel)) {
			handler.setOutputChannelName(defaultOutputChannel);
		}
		handler.afterPropertiesSet();
		channel.subscribe(handler);
	}

	@Override
	public void afterSingletonsInstantiated() {
		// Dump the mappings after the context has been created, ensuring that beans can
		// be processed correctly
		// again.
		this.mappedBindings.clear();
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @StreamListener method on the target class for this JDK proxy
				// ->
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
						"@StreamListener method '%s' found on bean target class '%s', "
								+ "but not found in any interface(s) for bean JDK proxy. Either "
								+ "pull the method up to an interface or switch to subclass (CGLIB) "
								+ "proxies by setting proxy-target-class/proxyTargetClass attribute to 'true'",
						method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	private final class StreamListenerMessageHandler extends AbstractReplyProducingMessageHandler {

		private final InvocableHandlerMethod invocableHandlerMethod;

		private StreamListenerMessageHandler(InvocableHandlerMethod invocableHandlerMethod) {
			this.invocableHandlerMethod = invocableHandlerMethod;
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			return false;
		}

		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			try {
				return this.invocableHandlerMethod.invoke(requestMessage);
			}
			catch (Exception e) {
				if (e instanceof MessagingException) {
					throw (MessagingException) e;
				}
				else {
					throw new MessagingException(requestMessage,
							"Exception thrown while invoking " + this.invocableHandlerMethod.getShortLogMessage(), e);
				}
			}
		}
	}

}
