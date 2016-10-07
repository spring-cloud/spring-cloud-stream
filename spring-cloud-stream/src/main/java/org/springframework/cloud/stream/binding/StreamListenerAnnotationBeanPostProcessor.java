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
					String outboundTargetValue = extractDefaultOutput(method);
					String inboundTargetValue = streamListener.value();
					Object[] arguments = new Object[method.getParameterTypes().length];
					assertStreamListenerMethod(method, arguments, inboundTargetValue, outboundTargetValue);
					boolean isDeclarative = false;
					for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
						Object targetBean = null;
						MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
						Class<?> parameterType = methodParameter.getParameterType();
						if (methodParameter.hasParameterAnnotation(Output.class)) {
							outboundTargetValue = (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
							targetBean = applicationContext.getBean(outboundTargetValue);
						}
						else if (methodParameter.hasParameterAnnotation(Input.class)) {
							inboundTargetValue = (String) AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
							targetBean = applicationContext.getBean(inboundTargetValue);
						}
						else { // get target from StreamListener
							targetBean = applicationContext.getBean(inboundTargetValue);
						}
						if (parameterType.isAssignableFrom(targetBean.getClass())) {
							arguments[parameterIndex] = targetBean;
							isDeclarative = true;
						}
						for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : streamListenerParameterAdapters) {
							if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
								arguments[parameterIndex] = streamListenerParameterAdapter.adapt(targetBean, methodParameter);
								isDeclarative = true;
								break;
							}
						}
					}
					Object outputAnnotatedBean = (StringUtils.hasText(outboundTargetValue)) ? applicationContext.getBean(outboundTargetValue) : null;
					if (!method.getReturnType().equals(Void.TYPE)) {
						Assert.notNull(outputAnnotatedBean, "StreamListener method with return type should have outbound target specified");
					}
					if (isDeclarative) {
						invokeSetupMethodOnListenedChannel(method, bean, arguments, outputAnnotatedBean);
					}
					else {
						// TBD: check for non message channel bound types
						registerHandlerMethodOnListenedChannel(method, inboundTargetValue, outboundTargetValue, bean);
					}
				}
			}
		});
		return bean;
	}

	private void assertStreamListenerMethod(Method method, Object[] arguments, String inboundBindingName, String outboundBindingName) {
		int inputAnnotationCount = 0, outputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				inputAnnotationCount++;
			}
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				outputAnnotationCount++;
			}
		}
		if (StringUtils.hasText(inboundBindingName)) {
			Assert.isTrue(inputAnnotationCount == 0, "Cannot set both StreamListener value " + inboundBindingName + "" +
					"and @Input annotation as method parameter");
		}
		else {
			if (inputAnnotationCount > 1) {
				throw new IllegalStateException("@Input annotation can not be used in multiple method parameters");
			}
			Assert.isTrue(inputAnnotationCount == 1, "Either StreamListener or a method parameter should be set with " +
					"an inbound element");
		}
		if (StringUtils.hasText(outboundBindingName)) {
			Assert.isTrue(outputAnnotationCount == 0, "Cannot set both Output (@Output/@SendTo) method annotation value " + outboundBindingName +
					" and @Output annotation as a method parameter");
		}
		Assert.isTrue(outputAnnotationCount <= 1, "@Output annotation can not be used in multiple method parameters");
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

	private String extractDefaultOutput(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), "At least one output must be specified");
			Assert.isTrue(sendTo.value().length == 1, "Multiple destinations cannot be specified");
			Assert.hasText(sendTo.value()[0], "An empty destination cannot be specified");
			return sendTo.value()[0];
		}
		Output output = AnnotationUtils.findAnnotation(method, Output.class);
		if (output != null) {
			Assert.isTrue(StringUtils.hasText(output.value()), "At least one output must be specified");
			return output.value();
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
