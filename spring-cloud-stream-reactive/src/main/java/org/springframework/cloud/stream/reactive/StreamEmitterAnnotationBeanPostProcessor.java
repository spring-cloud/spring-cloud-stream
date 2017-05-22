/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.reactive;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import reactor.core.Disposable;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.MessageChannelStreamListenerResultAdapter;
import org.springframework.cloud.stream.binding.StreamAnnotationCommonMethodUtils;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.SynthesizingMethodParameter;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link BeanPostProcessor} that handles {@link StreamEmitter} annotations found on bean methods.
 *
 * @author Soby Chacko
 * @since 1.3.0
 */
public class StreamEmitterAnnotationBeanPostProcessor
		implements BeanPostProcessor, InitializingBean, ApplicationContextAware, SmartLifecycle {

	private final List<StreamListenerParameterAdapter<?, Object>> streamListenerParameterAdapters = new ArrayList<>();

	private final List<StreamListenerResultAdapter<?, ?, ?>> streamListenerResultAdapters = new ArrayList<>();

	private final List<FluxSender> fluxSenders = new ArrayList<>();

	private final List<Disposable> fluxDisposables = new ArrayList<>();

	private ConfigurableApplicationContext applicationContext;

	private final List<Method> targetMethods = new ArrayList<>();

	private Object targetBean;

	private volatile boolean running;

	private Lock lock = new ReentrantLock();

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isTrue(applicationContext instanceof ConfigurableApplicationContext,
				"ConfigurableApplicationContext is required");
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void afterPropertiesSet() throws Exception {
		Map<String, StreamListenerParameterAdapter> parameterAdapterMap = BeanFactoryUtils
				.beansOfTypeIncludingAncestors(this.applicationContext, StreamListenerParameterAdapter.class);
		parameterAdapterMap.values().iterator().forEachRemaining(this.streamListenerParameterAdapters::add);
		Map<String, StreamListenerResultAdapter> resultAdapterMap = BeanFactoryUtils
				.beansOfTypeIncludingAncestors(this.applicationContext, StreamListenerResultAdapter.class);
		this.streamListenerResultAdapters.add(new MessageChannelStreamListenerResultAdapter());
		resultAdapterMap.values().iterator().forEachRemaining(this.streamListenerResultAdapters::add);
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
		ReflectionUtils.doWithMethods(targetClass, method -> {
			StreamEmitter streamEmitter = AnnotatedElementUtils.findMergedAnnotation(method, StreamEmitter.class);
			if (streamEmitter != null && !method.isBridge()) {
				targetMethods.add(method);
				targetBean = bean;
			}
		});
		return bean;
	}

	@Override
	public void start() {
		try {
			lock.lock();
			if (!running) {
				for (Method targetMethod : targetMethods) {
					Assert.isTrue(targetMethod.getAnnotation(Input.class) == null,
							StreamEmitterErrorMessages.INPUT_ANNOTATIONS_ARE_NOT_ALLOWED);
					String methodAnnotatedOutboundName = StreamAnnotationCommonMethodUtils.getOutboundBindingTargetName(targetMethod);
					int outputAnnotationCount = StreamAnnotationCommonMethodUtils.outputAnnotationCount(targetMethod);
					validateStreamEmitterMethod(targetMethod, outputAnnotationCount, methodAnnotatedOutboundName);
					invokeSetupMethodOnToTargetChannel(targetMethod, targetBean, methodAnnotatedOutboundName);
				}
				this.running = true;
			}
		}
		finally {
			lock.unlock();
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private void invokeSetupMethodOnToTargetChannel(Method method, Object bean, String outboundName) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		Object targetBean = null;
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = new SynthesizingMethodParameter(method, parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
			}
			else if (arguments.length == 1 && StringUtils.hasText(outboundName)) {
				targetReferenceValue = outboundName;
			}
			if (targetReferenceValue != null) {
				targetBean = this.applicationContext.getBean((String) targetReferenceValue);
				for (StreamListenerParameterAdapter<?, Object> streamListenerParameterAdapter : this.streamListenerParameterAdapters) {
					if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
						arguments[parameterIndex] = streamListenerParameterAdapter.adapt(targetBean,
								methodParameter);
						if (FluxSender.class.isAssignableFrom(arguments[parameterIndex].getClass())) {
							fluxSenders.add((FluxSender)arguments[parameterIndex]);
						}
						break;
					}
				}
				Assert.notNull(arguments[parameterIndex], "Cannot convert argument " + parameterIndex + " of " + method
						+ "from " + targetBean.getClass() + " to " + parameterType);
			}
			else {
				throw new IllegalStateException(StreamEmitterErrorMessages.ATLEAST_ONE_OUTPUT);
			}
		}
		Object result;
		try {
			result = method.invoke(bean, arguments);
		}
		catch (Exception e) {
			throw new BeanInitializationException("Cannot setup StreamEmitter for " + method, e);
		}

		if (!Void.TYPE.equals(method.getReturnType())) {
			if (targetBean == null) {
				targetBean = this.applicationContext.getBean(outboundName);
			}
			boolean streamListenerResultAdapterFound = false;
			for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
				if (streamListenerResultAdapter.supports(result.getClass(), targetBean.getClass())) {
					Object adapted = streamListenerResultAdapter.adapt(result, targetBean);
					if(adapted != null && Disposable.class.isAssignableFrom(adapted.getClass())) {
						fluxDisposables.add((Disposable)adapted);
					}
					streamListenerResultAdapterFound = true;
					break;
				}
			}
			Assert.state(streamListenerResultAdapterFound, StreamEmitterErrorMessages.CANNOT_CONVERT_RETURN_TYPE_TO_ANY_AVAILABLE_RESULT_ADAPTERS);
		}
	}

	private static void validateStreamEmitterMethod(Method method, int outputAnnotationCount, String methodAnnotatedOutboundName) {
		if (StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(outputAnnotationCount == 0,
					StreamEmitterErrorMessages.INVALID_OUTPUT_METHOD_PARAMETERS);
		}
		else {
			Assert.isTrue(outputAnnotationCount > 0, StreamEmitterErrorMessages.NO_OUTPUT_SPECIFIED);
		}

		if (!method.getReturnType().equals(Void.TYPE)) {
			Assert.isTrue(StringUtils.hasText(methodAnnotatedOutboundName),
					StreamEmitterErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
			Assert.isTrue(method.getParameterCount() == 0, StreamEmitterErrorMessages.RETURN_TYPE_METHOD_ARGUMENTS);
		}
		else {
			if (!StringUtils.hasText(methodAnnotatedOutboundName)) {
				int methodArgumentsLength = method.getParameterTypes().length;
				for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
					MethodParameter methodParameter = MethodParameter.forMethodOrConstructor(method, parameterIndex);
					if (methodParameter.hasParameterAnnotation(Output.class)) {
						String outboundName = (String) AnnotationUtils
								.getValue(methodParameter.getParameterAnnotation(Output.class));
						Assert.isTrue(StringUtils.hasText(outboundName), StreamEmitterErrorMessages.INVALID_OUTBOUND_NAME);
					}
					else {
						throw new IllegalArgumentException(
								StreamEmitterErrorMessages.OUTPUT_ANNOTATION_MISSING_ON_METHOD_PARAMETERS_VOID_RETURN_TYPE);
					}
				}
			}
		}
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public void stop() {
		try {
			lock.lock();
			for (FluxSender fluxSender : fluxSenders) {
				if (fluxSender.getClass().isAssignableFrom(DisposableFluxSender.class)) {
					((DisposableFluxSender) fluxSender).getDisposable().dispose();
				}
			}
			for (Disposable disposable : fluxDisposables) {
				disposable.dispose();
			}
			if (running) {
				this.running = false;
			}
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}
}
