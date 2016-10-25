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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import org.springframework.messaging.handler.annotation.Payload;
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
 * @author Ilayaperumal Gopinathan
 */
public class StreamListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton {

	private final DestinationResolver<MessageChannel> binderAwareChannelResolver;

	private final MessageHandlerMethodFactory messageHandlerMethodFactory;

	private final Map<String, Map<String, InvocableHandlerMethod>> mappedBindings = new HashMap<>();

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
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		Map<String, StreamListenerParameterAdapter> parameterAdapterMap =
				this.applicationContext.getBeansOfType(StreamListenerParameterAdapter.class);
		for (StreamListenerParameterAdapter parameterAdapter : parameterAdapterMap.values()) {
			this.streamListenerParameterAdapters.add(parameterAdapter);
		}
		Map<String, StreamListenerResultAdapter> resultAdapterMap =
				this.applicationContext.getBeansOfType(StreamListenerResultAdapter.class);
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
					if (!method.getReturnType().equals(Void.TYPE)) {
						Assert.isTrue(method.getAnnotation(Input.class) == null,
								"A @StreamListener may never be annotated with @Input." +
										"If it should listen to a specific input, use the value of @StreamListener " +
										"instead.");
					}
					Class<?>[] parameterTypes = method.getParameterTypes();
					if (StringUtils.hasText(streamListener.value())) {
						for (int i = 0; i < parameterTypes.length; i++) {
							MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, i);
							Assert.isTrue(methodParameter.getParameterAnnotation(Input.class) == null &&
											methodParameter.getParameterAnnotation(Output.class) == null,
									"A message handling @StreamListener method cannot have parameters annotated " +
											"with @Input or @Output");
						}
						if (!method.getReturnType().equals(Void.TYPE)) {
							Assert.isTrue(method.getAnnotation(Output.class) == null,
									"A message handling @StreamListener method cannot be annotated with @Output");
						}
						registerHandlerMethodOnListenedChannel(method, streamListener, bean);
					}
					else {
						for (int i = 0; i < parameterTypes.length; i++) {
							MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, i);
							Assert.isTrue(methodParameter.getParameterAnnotation(Input.class) != null ^
											methodParameter.getParameterAnnotation(Output.class) != null,
									"A declarative @StreamListener method must have its parameters annotated" +
											"with @Input or @Output, but not with both.");
						}
						if (!method.getReturnType().equals(Void.TYPE)) {
							Assert.isTrue(method.getAnnotation(Output.class) != null,
									"A declarative @StreamListener method must be annotated with @Output");
						}
						invokeSetupMethodOnListenedChannel(method, bean);
					}
				}
			}
		});
		return bean;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private void invokeSetupMethodOnListenedChannel(Method method, Object bean) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Annotation targetReferenceAnnotation = methodParameter.hasParameterAnnotation(Input.class) ?
					methodParameter.getParameterAnnotation(Input.class) : methodParameter.getParameterAnnotation(
					Output.class);
			Object targetReferenceAnnotationValue = AnnotationUtils.getValue(targetReferenceAnnotation);
			Assert.isInstanceOf(String.class, targetReferenceAnnotationValue, "Annotation value must be a String");
			Assert.hasText((String) targetReferenceAnnotationValue, "Annotation value must not be empty");
			Object targetBean = this.applicationContext.getBean((String) targetReferenceAnnotationValue);
			if (parameterType.isAssignableFrom(targetBean.getClass())) {
				arguments[parameterIndex] = targetBean;
			}
			else {
				for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter :
						this.streamListenerParameterAdapters) {
					if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
						arguments[parameterIndex] = streamListenerParameterAdapter.adapt(targetBean, methodParameter);
						break;
					}
				}
			}
			Assert.notNull(arguments[parameterIndex],
					"Cannot convert argument " + parameterIndex + " of " + method + "from " + targetBean.getClass()
							.toString() + " to " + parameterType.toString());
		}
		try {
			if (method.getReturnType().equals(Void.TYPE)) {
				method.invoke(bean, arguments);
			}
			else {
				Object result = method.invoke(bean, arguments);
				Output output = AnnotationUtils.getAnnotation(method, Output.class);
				Object targetBean = this.applicationContext.getBean(output.value());
				for (StreamListenerResultAdapter streamListenerResultAdapter : this
						.streamListenerResultAdapters) {
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
		final InvocableHandlerMethod invocableHandlerMethod =
				this.messageHandlerMethodFactory.createInvocableHandlerMethod(bean, targetMethod);
		if (!StringUtils.hasText(streamListener.value())) {
			throw new BeanInitializationException("A bound component name must be specified");
		}
		HashMap<String, InvocableHandlerMethod> handlerMethods = (HashMap<String, InvocableHandlerMethod>) ((this.mappedBindings.get(streamListener.value()) != null) ?
						(this.mappedBindings.get(streamListener.value())) : new HashMap<>());
		if (this.mappedBindings.containsKey(streamListener.value())) {
			boolean duplicateMapping = false;
			for (InvocableHandlerMethod existingHandlerMethod : handlerMethods.values()) {
				for (MethodParameter existingMethodParameter : existingHandlerMethod.getMethodParameters()) {
					// Exclude method parameters that use MessageMapping annotations
					// and only include the target payload
					if (isValidParameterForPayloadTarget(existingMethodParameter)) {
						for (MethodParameter currentMethodParameter : invocableHandlerMethod.getMethodParameters()) {
							if (isValidParameterForPayloadTarget(currentMethodParameter)) {
								if (existingMethodParameter.getParameterType().isAssignableFrom(currentMethodParameter.getParameterType())) {
									duplicateMapping = true;
									break;
								}
								else if (currentMethodParameter.getParameterType().isAssignableFrom(existingMethodParameter.getParameterType())) {
									duplicateMapping = true;
									break;
								}
								else if (currentMethodParameter.getGenericParameterType() instanceof ParameterizedType &&
										currentMethodParameter.getParameterType().isAssignableFrom(Message.class)) {
									duplicateMapping = isDuplicate(currentMethodParameter, existingMethodParameter);
									break;
								}
								else if (existingMethodParameter.getGenericParameterType() instanceof ParameterizedType &&
										existingMethodParameter.getParameterType().isAssignableFrom(Message.class)) {
									duplicateMapping = isDuplicate(existingMethodParameter, currentMethodParameter);
									break;
								}
							}
						}
					}
				}
				if (duplicateMapping) {
					throw new BeanInitializationException("Duplicate @" + StreamListener.class.getSimpleName() +
							" mapping for '" + streamListener.value() + "' on " + invocableHandlerMethod.getShortLogMessage() +
							" already existing for " + existingHandlerMethod.getShortLogMessage());
				}
			}
		}
		handlerMethods.put(invocableHandlerMethod.getMethod().getName() + "-" + new Random().nextInt(), invocableHandlerMethod);
		this.mappedBindings.put(streamListener.value(), handlerMethods);
		SubscribableChannel channel = this.applicationContext.getBean(streamListener.value(),
				SubscribableChannel.class);
		final String defaultOutputChannel = extractDefaultOutput(method);
		if (invocableHandlerMethod.isVoid()) {
			Assert.isTrue(StringUtils.isEmpty(defaultOutputChannel),
					"An output channel cannot be specified for a method that " +
							"does not return a value");
		}
		else {
			Assert.isTrue(!StringUtils.isEmpty(defaultOutputChannel),
					"An output channel must be specified for a method that " +
							"can return a value");
		}
		StreamListenerMessageHandler handler = new StreamListenerMessageHandler(invocableHandlerMethod);
		handler.setApplicationContext(this.applicationContext);
		handler.setChannelResolver(this.binderAwareChannelResolver);
		if (!StringUtils.isEmpty(defaultOutputChannel)) {
			handler.setOutputChannelName(defaultOutputChannel);
		}
		handler.afterPropertiesSet();
		channel.subscribe(handler);
	}

	private boolean isValidParameterForPayloadTarget(MethodParameter methodParameter) {
		return !methodParameter.hasParameterAnnotations() ||
				methodParameter.hasParameterAnnotation(Input.class) ||
				methodParameter.hasParameterAnnotation(Payload.class);
	}

	private boolean isDuplicate(MethodParameter methodParameter1, MethodParameter methodParameter2) {
		Type[] genericTypes = ((ParameterizedType) methodParameter1.getGenericParameterType()).getActualTypeArguments();
		if (genericTypes.length > 0) {
			Class<?> genericType = (Class<?>) genericTypes[0];
			if (methodParameter2.getParameterType().isAssignableFrom(genericType)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void afterSingletonsInstantiated() {
		// Dump the mappings after the context has been created, ensuring that beans can be processed correctly
		// again.
		this.mappedBindings.clear();
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
					throw new MessagingException(requestMessage, "Exception thrown while invoking " + this
							.invocableHandlerMethod
							.getShortLogMessage(), e);
				}
			}
		}
	}

}
