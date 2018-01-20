/*
 * Copyright 2018 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.kafka.streams.kstream.KStream;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.binding.StreamListenerSetupMethodOrchestrator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Kafka Streams specific implementation for {@link StreamListenerSetupMethodOrchestrator}
 * that overrides the default mechanisms for invoking StreamListener adapters.
 *
 * @since 2.0.0
 * 
 * @author Soby Chacko
 */
public class KStreamListenerSetupMethodOrchestrator implements StreamListenerSetupMethodOrchestrator, ApplicationContextAware {

	private ConfigurableApplicationContext applicationContext;

	private StreamListenerParameterAdapter streamListenerParameterAdapter;
	private Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

	public KStreamListenerSetupMethodOrchestrator(StreamListenerParameterAdapter streamListenerParameterAdapter,
												Collection<StreamListenerResultAdapter> streamListenerResultAdapters) {
		this.streamListenerParameterAdapter = streamListenerParameterAdapter;
		this.streamListenerResultAdapters = streamListenerResultAdapters;
	}

	@Override
	public boolean supports(Method method) {
		return methodParameterSuppports(method) && methodReturnTypeSuppports(method);
	}

	private boolean methodReturnTypeSuppports(Method method) {
		Class<?> returnType = method.getReturnType();
		if (returnType.equals(KStream.class) ||
				(returnType.isArray() && returnType.getComponentType().equals(KStream.class))) {
			return true;
		}
		return false;
	}

	private boolean methodParameterSuppports(Method method) {
		MethodParameter methodParameter = MethodParameter.forExecutable(method, 0);
		Class<?> parameterType = methodParameter.getParameterType();
		return parameterType.equals(KStream.class);
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void orchestrateStreamListenerSetupMethod(StreamListener streamListener, Method method, Object bean) {
		String[] methodAnnotatedOutboundNames = getOutboundBindingTargetNames(method);
		validateStreamListenerMethod(streamListener, method, methodAnnotatedOutboundNames);

		String methodAnnotatedInboundName = streamListener.value();
		Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(method, methodAnnotatedInboundName,
				this.applicationContext,
				this.streamListenerParameterAdapter);

		try {
			Object result = method.invoke(bean, adaptedInboundArguments);

			if (result.getClass().isArray()) {
				Assert.isTrue(methodAnnotatedOutboundNames.length == ((Object[]) result).length, "Big error");
			} else {
				Assert.isTrue(methodAnnotatedOutboundNames.length == 1, "Big error");
			}
			if (result.getClass().isArray()) {
				Object[] outboundKStreams = (Object[]) result;
				int i = 0;
				for (Object outboundKStream : outboundKStreams) {
					Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[i++]);
					for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
						if (streamListenerResultAdapter.supports(outboundKStream.getClass(), targetBean.getClass())) {
							streamListenerResultAdapter.adapt(outboundKStream, targetBean);
							break;
						}
					}
				}
			}
			else {
				Object targetBean = this.applicationContext.getBean(methodAnnotatedOutboundNames[0]);
				for (StreamListenerResultAdapter streamListenerResultAdapter : streamListenerResultAdapters) {
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

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	private void validateStreamListenerMethod(StreamListener streamListener, Method method, String[] methodAnnotatedOutboundNames) {
		String methodAnnotatedInboundName = streamListener.value();
		for (String s : methodAnnotatedOutboundNames) {
			if (StringUtils.hasText(s)) {
				Assert.isTrue(isDeclarativeOutput(method, s), "Method must be declarative");
			}
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			int methodArgumentsLength = method.getParameterTypes().length;

			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
				Assert.isTrue(isDeclarativeInput(methodAnnotatedInboundName, methodParameter), "Method must be declarative");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeOutput(Method m, String targetBeanName) {
		boolean declarative;
		Class<?> returnType = m.getReturnType();
		if (returnType.isArray()){
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			declarative = this.streamListenerResultAdapters.stream()
					.anyMatch(slpa -> slpa.supports(returnType.getComponentType(), targetBeanClass));
			return declarative;
		}
		Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
		declarative = this.streamListenerResultAdapters.stream()
				.anyMatch(slpa -> slpa.supports(returnType, targetBeanClass));
		return declarative;
	}

	@SuppressWarnings("unchecked")
	private boolean isDeclarativeInput(String targetBeanName, MethodParameter methodParameter) {
		if (!methodParameter.getParameterType().isAssignableFrom(Object.class) && this.applicationContext.containsBean(targetBeanName)) {
			Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
			return this.streamListenerParameterAdapter.supports(targetBeanClass, methodParameter);
		}
		return false;
	}

	private static String[] getOutboundBindingTargetNames(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length >= 1, "At least one outbound destination need to be provided.");
			return sendTo.value();
		}
		return null;
	}
}
