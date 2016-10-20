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
					Assert.isTrue(method.getAnnotation(Input.class) == null,
								"A @StreamListener may never be annotated with @Input." +
										"If it should listen to a specific input, use the value of @StreamListener " +
										"instead.");
					String outboundElementName = getOutboundElementNameFromMethodAnnotation(method);
					String inboundElementName = getInboundElementNameFromMethodAnnotation(streamListener);
					// Perform assertion on StreamListener method based on the inbound/outbound element names
					// from method annotation and method parameters
					assertStreamListenerMethod(method, inboundElementName, outboundElementName);
					inboundElementName = (inboundElementName != null) ? inboundElementName : getInboundElementNameFromMethodParameter(method);
					outboundElementName = (outboundElementName != null) ? outboundElementName : getOutboundElementNameFromMethodParameter(method);
					Object inboundTargetBean = (inboundElementName != null) ? getBindableBean(inboundElementName) : null;
					Object outboundTargetBean = (outboundElementName != null) ? getBindableBean(outboundElementName) : null;
					if (!method.getReturnType().equals(Void.TYPE)) {
						Assert.notNull(outboundTargetBean, "StreamListener method with return type should have outbound target specified");
					}
					Object[] declarativeArguments = new Object[method.getParameterTypes().length];
					boolean isDeclarative = false;
					for (int parameterIndex = 0; parameterIndex < declarativeArguments.length; parameterIndex++) {
						MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
						if (methodParameter.hasParameterAnnotation(Input.class) || !methodParameter.hasParameterAnnotation(Output.class)) {
							if (isDeclarativeMethodParameter(inboundTargetBean, methodParameter)) {
								declarativeArguments[parameterIndex] = getDeclarativeArgument(inboundTargetBean, methodParameter);
								Assert.notNull(declarativeArguments[parameterIndex],
										"Cannot convert argument " + parameterIndex + " of " + method + "from " + inboundTargetBean.getClass()
												.toString() + " to " + methodParameter.getParameterType().toString());
								isDeclarative = true;
							}
						}
						else if (outboundTargetBean != null && methodParameter.hasParameterAnnotation(Output.class)) {
							if (isDeclarativeMethodParameter(outboundTargetBean, methodParameter)) {
								declarativeArguments[parameterIndex] = getDeclarativeArgument(outboundTargetBean, methodParameter);
								Assert.notNull(declarativeArguments[parameterIndex],
										"Cannot convert argument " + parameterIndex + " of " + method + "from " + outboundTargetBean.getClass()
												.toString() + " to " + methodParameter.getParameterType().toString());
								isDeclarative = true;
							}
						}
					}
					if (isDeclarative) {
						assertDeclarativeArguments(declarativeArguments);
						invokeSetupMethodOnListenedChannel(method, bean, declarativeArguments, outboundTargetBean);
					}
					else {
						// TBD: check for non message channel bound types
						registerHandlerMethodOnListenedChannel(method, inboundElementName, outboundElementName, bean);
					}
				}
			}
		});
		return bean;
	}

	private boolean isDeclarativeMethodParameter(Object targetBean, MethodParameter methodParameter) {
		if (methodParameter.getParameterType().isAssignableFrom(targetBean.getClass())) {
			return true;
		}
		for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : streamListenerParameterAdapters) {
			if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
				return true;
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

	private void assertStreamListenerMethod(Method method, String inboundBindingName, String outboundBindingName) {
		int inputAnnotationCount = 0, outputAnnotationCount = 0;
		int argumentsLength = method.getParameterTypes().length;
		for (int parameterIndex = 0; parameterIndex < argumentsLength; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				inputAnnotationCount++;
			}
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				outputAnnotationCount++;
			}
		}
		Assert.isTrue(outputAnnotationCount <= 1, "@Output annotation can not be used in multiple method parameters");
		Assert.isTrue(inputAnnotationCount <= 1, "@Input annotation can not be used in multiple method parameters");
		if (StringUtils.hasText(inboundBindingName)) {
			Assert.isTrue(inputAnnotationCount == 0, "Cannot set both StreamListener value " + inboundBindingName + "" +
					"and @Input annotation as method parameter");
			if (outputAnnotationCount > 0) {
				Assert.isTrue(inputAnnotationCount > 0, "@Input annotation should be specified as method parameter instead of " +
						"StreamListener value when specifying @Output/@SendTo annotation");
			}
		}
		else {
			Assert.isTrue(inputAnnotationCount == 1, "Either StreamListener or a method parameter should be set with " +
					"an inbound element");
		}
		if (StringUtils.hasText(outboundBindingName)) {
			Assert.isTrue(outputAnnotationCount == 0, "Cannot set both Output (@Output/@SendTo) method annotation value " + outboundBindingName +
					" and @Output annotation as a method parameter");
		}
		// When both inbound and outbound method parameter specified
		if (argumentsLength > 1 && (inputAnnotationCount > 0 || outputAnnotationCount > 0)) {
			Assert.isTrue(inputAnnotationCount == outputAnnotationCount, "Declarative StreamListener method with both " +
					"inbound and outbound values should have @Input and @Output annotations specified as method parameters");
			}
	}

	private String getInboundElementNameFromMethodParameter(Method method) {
		for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				return (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
			}
		}
		return null;
	}

	private String getOutboundElementNameFromMethodParameter(Method method) {
		for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				return (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
			}
		}
		return null;
	}

	private Object getDeclarativeArgument(Object targetBean, MethodParameter methodParameter) {
		if (methodParameter.getParameterType().isAssignableFrom(targetBean.getClass())) {
			return getBindableArgument(targetBean, methodParameter);
		}
		for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : streamListenerParameterAdapters) {
			if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
				return streamListenerParameterAdapter.adapt(targetBean, methodParameter);
			}
		}
		return null;
	}

	private Object getBindableBean(String elementName) {
		try {
			return this.applicationContext.getBean(elementName);
		}
		catch(NoSuchBeanDefinitionException e) {
			throw new IllegalStateException("Bindable target is not found for the name: " + elementName);
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

	private void invokeSetupMethodOnListenedChannel(Method method, Object bean, Object[] arguments, Object outputAnnotatedBean) {
		try {
			if (method.getReturnType().equals(Void.TYPE)) {
				method.invoke(bean, arguments);
			}
			else {
				Object result = method.invoke(bean, arguments);
				for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
					if (outputAnnotatedBean != null) {
						if (streamListenerResultAdapter.supports(result.getClass(), outputAnnotatedBean.getClass())) {
							streamListenerResultAdapter.adapt(result, outputAnnotatedBean);
							break;
						}
					}
				}
			}
		}
		catch (Exception e) {
			throw new BeanInitializationException("Cannot setup StreamListener for " + method, e);
		}
	}

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
