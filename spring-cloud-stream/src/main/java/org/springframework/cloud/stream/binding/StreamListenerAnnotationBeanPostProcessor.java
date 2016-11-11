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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
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

	private final Map<String, InvocableHandlerMethod> mappedBindings = new HashMap<>();

	private final Map<String, Object> boundElements = new HashMap<>();

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

//	@Override
//	public Object postProcessAfterInitialization(final Object bean, String beanName) throws BeansException {
//		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
//		ReflectionUtils.doWithMethods(targetClass, new ReflectionUtils.MethodCallback() {
//			@Override
//			public void doWith(final Method method) throws IllegalArgumentException, IllegalAccessException {
//				StreamListener streamListener = AnnotationUtils.findAnnotation(method, StreamListener.class);
//				if (streamListener != null) {
//					Assert.isTrue(method.getAnnotation(Input.class) == null,
//							"A @StreamListener may never be annotated with @Input. If it should listen to a specific input, " +
//									"use the value of @StreamListener instead.");
//					String methodAnnotatedInboundName = getInboundElementNameFromMethodAnnotation(streamListener);
//					String methodAnnotatedOutboundName = getOutboundElementNameFromMethodAnnotation(method);
//					int inputAnnotationCount = inputAnnotationCount(method);
//					int outputAnnotationCount = outputAnnotationCount(method);
//					boolean isDeclarative = isDeclarativeStreamListenerMethod(method, methodAnnotatedInboundName, methodAnnotatedOutboundName);
//					assertStreamListenerMethod(inputAnnotationCount, outputAnnotationCount, methodAnnotatedInboundName, methodAnnotatedOutboundName, isDeclarative);
//					Object outboundTargetBean = null;
//					if (StringUtils.hasText(methodAnnotatedOutboundName)) {
//						outboundTargetBean = getBindableBean(methodAnnotatedOutboundName);
//					}
//					if (!method.getReturnType().equals(Void.TYPE)) {
//						Assert.isTrue((outputAnnotationCount <= 1),
//								"StreamListener method with return type should have only one outbound target specified");
//						Assert.notNull(outboundTargetBean, "StreamListener method with return type should have outbound target specified");
//					}
//					int methodArgumentsLength = method.getParameterTypes().length;
//					if (!isDeclarative) {
//						assertStreamListenerMessageHandlerMethod(method, methodAnnotatedInboundName);
//						registerHandlerMethodOnListenedChannel(method, methodAnnotatedInboundName, methodAnnotatedOutboundName, bean);
//					}
//					else {
//						Object[] declarativeArguments = new Object[methodArgumentsLength];
//						for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
//							MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
//							Object targetBean = null;
//							if (methodParameter.hasParameterAnnotation(Input.class)) {
//								String inboundElementName = (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
//								Assert.isTrue(StringUtils.hasText(inboundElementName), "@Input annotation should always be associated with a valid inbound name");
//								targetBean = getBindableBean(inboundElementName);
//								declarativeArguments[parameterIndex] = getDeclarativeArgument(targetBean, methodParameter);
//							}
//							else if (methodParameter.hasParameterAnnotation(Output.class)) {
//								String outboundElementName = (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
//								Assert.isTrue(StringUtils.hasText(outboundElementName), "@Output annotation should always be associated with a valid outbound name");
//								targetBean = getBindableBean(outboundElementName);
//								declarativeArguments[parameterIndex] = getDeclarativeArgument(targetBean, methodParameter);
//							}
//							else if (declarativeArguments.length == 1 && StringUtils.hasText(methodAnnotatedInboundName)) {
//								targetBean = getBindableBean(methodAnnotatedInboundName);
//								declarativeArguments[parameterIndex] = getDeclarativeArgument(targetBean, methodParameter);
//							}
//							Assert.isTrue(declarativeArguments[parameterIndex] != null, "Declarative StreamListener method should only have " +
//									"inbound or outbound targets as method parameters");
//							Assert.notNull(declarativeArguments[parameterIndex],
//									"Cannot convert argument " + parameterIndex + " of " + method + "from " + targetBean.getClass()
//											.toString() + " to " + methodParameter.getParameterType().toString());
//						}
//						assertDeclarativeArguments(declarativeArguments);
//						invokeSetupMethodOnListenedChannel(method, bean, declarativeArguments, outboundTargetBean);
//					}
//				}
//			}
//		});
//		return bean;
//	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, String beanName) throws BeansException {
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
		ReflectionUtils.doWithMethods(targetClass, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(final Method method) throws IllegalArgumentException, IllegalAccessException {
				StreamListener streamListener = AnnotationUtils.findAnnotation(method, StreamListener.class);
				if (streamListener != null) {
					Assert.isTrue(method.getAnnotation(Input.class) == null,
							"A @StreamListener may never be annotated with @Input. If it should listen to a specific input, " +
									"use the value of @StreamListener instead.");
					String methodAnnotatedInboundName = getInboundElementNameFromMethodAnnotation(streamListener);
					String methodAnnotatedOutboundName = getOutboundElementNameFromMethodAnnotation(method);
					int inputAnnotationCount = inputAnnotationCount(method);
					int outputAnnotationCount = outputAnnotationCount(method);
					boolean isDeclarative = isDeclarativeStreamListenerMethod(method, methodAnnotatedInboundName, methodAnnotatedOutboundName);
					assertStreamListenerMethod(inputAnnotationCount, outputAnnotationCount, methodAnnotatedInboundName, methodAnnotatedOutboundName, isDeclarative);
					if (!method.getReturnType().equals(Void.TYPE)) {
						Assert.isTrue(method.getAnnotation(Input.class) == null,
								"A @StreamListener may never be annotated with @Input." +
										"If it should listen to a specific input, use the value of @StreamListener " +
										"instead.");
						Assert.isTrue((outputAnnotationCount <= 1),
								"StreamListener method with return type should have only one outbound target specified");
					}
					Class<?>[] parameterTypes = method.getParameterTypes();
					if (!StringUtils.hasText(streamListener.value()) && isDeclarative) {
						invokeSetupMethodOnListenedChannel(method, bean);
					}
					else {
						if (parameterTypes.length == 1) {
							if (isDeclarative) {
								invokeSetupMethodOnListenedChannel(method, bean);
							}
							else {
								registerHandlerMethodOnListenedChannel(method, streamListener, bean);
							}
						}
						else {
							registerHandlerMethodOnListenedChannel(method, streamListener, bean);
						}
					}
//					if (StringUtils.hasText(streamListener.value())) {
//						for (int i = 0; i < parameterTypes.length; i++) {
//							MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, i);
//							Assert.isTrue(methodParameter.getParameterAnnotation(Input.class) == null &&
//											methodParameter.getParameterAnnotation(Output.class) == null,
//									"A message handling @StreamListener method cannot have parameters annotated " +
//											"with @Input or @Output");
//						}
//						if (!method.getReturnType().equals(Void.TYPE)) {
//							Assert.isTrue(method.getAnnotation(Output.class) == null,
//									"A message handling @StreamListener method cannot be annotated with @Output");
//						}
//						registerHandlerMethodOnListenedChannel(method, streamListener, bean);
//					}
//					else {
//						for (int i = 0; i < parameterTypes.length; i++) {
//							MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, i);
//							Assert.isTrue(methodParameter.getParameterAnnotation(Input.class) != null ^
//											methodParameter.getParameterAnnotation(Output.class) != null,
//									"A declarative @StreamListener method must have its parameters annotated" +
//											"with @Input or @Output, but not with both.");
//						}
//						if (!method.getReturnType().equals(Void.TYPE)) {
//							Assert.isTrue(method.getAnnotation(Output.class) != null,
//									"A declarative @StreamListener method must be annotated with @Output");
//						}
//						invokeSetupMethodOnListenedChannel(method, bean);
//					}
				}
			}
		});
		return bean;
	}

	private int inputAnnotationCount(Method method) {
		int inputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				inputAnnotationCount++;
			}
		}
		return inputAnnotationCount;
	}

	private int outputAnnotationCount(Method method) {
		int outputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				outputAnnotationCount++;
			}
		}
		return outputAnnotationCount;
	}

	private boolean isDeclarativeStreamListenerMethod(Method method, String methodAnnotatedinboundName, String methodAnnotatedOutboundName) {
		int methodArgumentsLength = method.getParameterTypes().length;
		for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				String inboundName = (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
				Assert.isTrue(StringUtils.hasText(inboundName), "@Input annotation should always be associated with a valid inbound name");
				return isDeclarativeMethodParameter(getBindableBean(inboundName), methodParameter);
			}
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				String outboundName = (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
				Assert.isTrue(StringUtils.hasText(outboundName), "@Output annotation should always be associated with a valid outbound name");
				return isDeclarativeMethodParameter(getBindableBean(outboundName), methodParameter);
			}
			if (StringUtils.hasText(methodAnnotatedOutboundName)) {
				return isDeclarativeMethodParameter(getBindableBean(methodAnnotatedOutboundName), methodParameter);
			}
			if (StringUtils.hasText(methodAnnotatedinboundName)) {
				return isDeclarativeMethodParameter(getBindableBean(methodAnnotatedinboundName), methodParameter);
			}
		}
		return false;
	}

	private boolean isDeclarativeMethodParameter(Object targetBean, MethodParameter methodParameter) {
		if (targetBean != null) {
			if (methodParameter.getParameterType().isAssignableFrom(targetBean.getClass())) {
				return true;
			}
			for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : streamListenerParameterAdapters) {
				if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
					return true;
				}
			}
		}
		return false;
	}

	private String getInboundElementNameFromMethodAnnotation(StreamListener streamListener) {
		return StringUtils.hasText(streamListener.value()) ? streamListener.value() : null;
	}

	private String getOutboundElementNameFromMethodAnnotation(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), "At least one output must be specified");
			Assert.isTrue(sendTo.value().length == 1, "Multiple destinations cannot be specified");
			Assert.hasText(sendTo.value()[0], "An empty destination cannot be specified");
			return StringUtils.hasText(sendTo.value()[0]) ? sendTo.value()[0] : null;
		}
		Output output = AnnotationUtils.findAnnotation(method, Output.class);
		if (output != null) {
			Assert.isTrue(StringUtils.hasText(output.value()), "At least one output must be specified");
			return StringUtils.hasText(output.value()) ? output.value() : null;
		}
		return null;
	}

	private void assertStreamListenerMethod(int inputAnnotationCount, int outputAnnotationCount,
			String methodAnnotatedInboundName, String methodAnnotatedOutboundName, boolean isDeclarative) {
		if (!isDeclarative) {
			Assert.isTrue(inputAnnotationCount == 0 && outputAnnotationCount == 0,
					"@Input or @Output annotation is not supported as method parameter in StreamListener method with " +
							"message handler mapping");
		}
		if (StringUtils.hasText(methodAnnotatedInboundName) && StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(inputAnnotationCount == 0 && outputAnnotationCount == 0, "@Input or @Output annotations are not permitted as " +
					"method parameters when both inbound and outbound values are set as method annotated values");
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			Assert.isTrue(inputAnnotationCount == 0, "Cannot set both StreamListener value " + methodAnnotatedInboundName +
					" and @Input annotation as method parameter");
			Assert.isTrue(outputAnnotationCount == 0, "Cannot set StreamListener value '" + methodAnnotatedInboundName + "' when using" +
					" @Output annotation as method parameter. Use @Input method parameter annotation to specify inbound value instead");
		}
		else {
			Assert.isTrue(inputAnnotationCount >= 1, "No input destination is configured. Use either a @StreamListener argument or @Input");
		}
		if (StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(outputAnnotationCount == 0, "Cannot set both Output (@Output/@SendTo) method annotation value '" + methodAnnotatedOutboundName +
					"' and @Output annotation as a method parameter");
		}
	}

	private void assertStreamListenerMessageHandlerMethod(Method method, String inboundName) {
		Assert.isTrue(StringUtils.hasText(inboundName), "No input destination is configured. Use @StreamListener argument");
		int methodArgumentsLength = method.getParameterTypes().length;
		if (methodArgumentsLength > 1) {
			int numAnnotatedMethodParameters = 0;
			int numPayloadAnnotations = 0;
			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
				if (methodParameter.hasParameterAnnotations()) {
					numAnnotatedMethodParameters++;
				}
				if (methodParameter.hasParameterAnnotation(Payload.class)) {
					numPayloadAnnotations++;
				}
			}
			Assert.isTrue(methodArgumentsLength <= numAnnotatedMethodParameters && numPayloadAnnotations <= 1,
					"Ambiguous method arguments for the StreamListener method");
		}
	}

	private Object getDeclarativeArgument(Object targetBean, MethodParameter methodParameter) {
		if (targetBean != null) {
			if (methodParameter.getParameterType().isAssignableFrom(targetBean.getClass())) {
				return getBindableArgument(targetBean, methodParameter);
			}
			for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : streamListenerParameterAdapters) {
				if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
					return streamListenerParameterAdapter.adapt(targetBean, methodParameter);
				}
			}
		}
		return null;
	}

	private Object getBindableBean(String boundElementName) {
		try {
			if (!this.boundElements.containsKey(boundElementName)) {
				this.boundElements.put(boundElementName, this.applicationContext.getBean(boundElementName));
			}
			return this.boundElements.get(boundElementName);
		}
		catch (NoSuchBeanDefinitionException e) {
			throw new IllegalStateException("Target bean doesn't exist for the bound element name: " + boundElementName);
		}
	}

	private Object getBindableArgument(Object targetBean, MethodParameter methodParameter) {
		if (methodParameter.getParameterType().isAssignableFrom(targetBean.getClass())) {
			return targetBean;
		}
		for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : streamListenerParameterAdapters) {
			if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
				return streamListenerParameterAdapter.adapt(targetBean, methodParameter);
			}
		}
		throw new IllegalStateException("Bindable target is not found for the target " + targetBean.getClass() + " and" +
				"the method parameter " + methodParameter.toString());
	}

	private void assertDeclarativeArguments(Object[] declarativeArguments) {
		for (int parameterIndex = 0; parameterIndex < declarativeArguments.length; parameterIndex++) {
			if (declarativeArguments[parameterIndex] == null) {
				throw new IllegalStateException("Declarative StreamListener method should only have inbound or outbound targets as method parameters");
			}
		}
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
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				Assert.isTrue(StringUtils.hasText((String) targetReferenceAnnotationValue), "@Output annotation should always be associated with a valid outbound name");
			}
			Object targetBean = getBindableBean((String) targetReferenceAnnotationValue);
			Assert.notNull(targetBean, "StreamListener method with return type should have outbound target specified");
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
		assertDeclarativeArguments(arguments);
		try {
			if (method.getReturnType().equals(Void.TYPE)) {
				method.invoke(bean, arguments);
			}
			else {
				Object result = method.invoke(bean, arguments);
				String outputValue = getOutboundElementNameFromMethodAnnotation(method);
				if (!StringUtils.hasText(outputValue)) {
					Output output = AnnotationUtils.getAnnotation(method, Output.class);
					outputValue = output.value();
					Assert.isTrue(StringUtils.hasText(outputValue), "@Output annotation should always be associated with a valid outbound name");
				}
				Object targetBean = this.applicationContext.getBean(outputValue);
				Assert.notNull(targetBean, "StreamListener method with return type should have outbound target specified");
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
		if (this.mappedBindings.containsKey(streamListener.value())) {
			throw new BeanInitializationException("Duplicate @" + StreamListener.class.getSimpleName() +
					" mapping for '" + streamListener.value() + "' on " + invocableHandlerMethod.getShortLogMessage() +
					" already existing for " + this.mappedBindings.get(streamListener.value()).getShortLogMessage());
		}
		this.mappedBindings.put(streamListener.value(), invocableHandlerMethod);
		SubscribableChannel channel = this.applicationContext.getBean(streamListener.value(),
				SubscribableChannel.class);
		final String defaultOutputChannel = getOutboundElementNameFromMethodAnnotation(method);
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
		assertStreamListenerMessageHandlerMethod(method, streamListener.value());
		StreamListenerMessageHandler handler = new StreamListenerMessageHandler(invocableHandlerMethod);
		handler.setApplicationContext(this.applicationContext);
		handler.setChannelResolver(this.binderAwareChannelResolver);
		if (!StringUtils.isEmpty(defaultOutputChannel)) {
			handler.setOutputChannelName(defaultOutputChannel);
		}
		handler.afterPropertiesSet();
		channel.subscribe(handler);
	}

//	private void invokeSetupMethodOnListenedChannel(Method method, Object bean, Object[] arguments, Object outputAnnotatedBean) {
//		try {
//			if (method.getReturnType().equals(Void.TYPE)) {
//				method.invoke(bean, arguments);
//			}
//			else {
//				Object result = method.invoke(bean, arguments);
//				for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
//					if (outputAnnotatedBean != null) {
//						if (streamListenerResultAdapter.supports(result.getClass(), outputAnnotatedBean.getClass())) {
//							streamListenerResultAdapter.adapt(result, outputAnnotatedBean);
//							break;
//						}
//					}
//				}
//			}
//		}
//		catch (Exception e) {
//			throw new BeanInitializationException("Cannot setup StreamListener for " + method, e);
//		}
//	}

	protected void registerHandlerMethodOnListenedChannel(Method method, String inboundBindingName,
			String outboundBindingName, Object bean) {
		Method targetMethod = checkProxy(method, bean);
		Assert.hasText(inboundBindingName, "The binding name cannot be null");
		final InvocableHandlerMethod invocableHandlerMethod =
				this.messageHandlerMethodFactory.createInvocableHandlerMethod(bean, targetMethod);
		if (!StringUtils.hasText(inboundBindingName)) {
			throw new BeanInitializationException("A bound component name must be specified");
		}
		if (this.mappedBindings.containsKey(inboundBindingName)) {
			throw new BeanInitializationException("Duplicate @" + StreamListener.class.getSimpleName() +
					" mapping for '" + inboundBindingName + "' on " + invocableHandlerMethod.getShortLogMessage() +
					" already existing for " + this.mappedBindings.get(inboundBindingName).getShortLogMessage());
		}
		this.mappedBindings.put(inboundBindingName, invocableHandlerMethod);
		SubscribableChannel channel = this.applicationContext.getBean(inboundBindingName, SubscribableChannel.class);
		StreamListenerMessageHandler handler = new StreamListenerMessageHandler(invocableHandlerMethod);
		handler.setApplicationContext(this.applicationContext);
		handler.setChannelResolver(this.binderAwareChannelResolver);
		if (!StringUtils.isEmpty(outboundBindingName)) {
			handler.setOutputChannelName(outboundBindingName);
		}
		handler.afterPropertiesSet();
		channel.subscribe(handler);
	}

	@Override
	public void afterSingletonsInstantiated() {
		// Dump the mappings after the context has been created, ensuring that beans can be processed correctly
		// again.
		this.mappedBindings.clear();
		this.boundElements.clear();
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
