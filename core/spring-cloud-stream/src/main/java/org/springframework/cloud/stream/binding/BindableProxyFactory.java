/*
 * Copyright 2015-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * {@link FactoryBean} for instantiating the interfaces specified via
 * {@link EnableBinding}.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @see EnableBinding
 */
public class BindableProxyFactory extends AbstractBindableProxyFactory
		implements MethodInterceptor, FactoryBean<Object>, InitializingBean, BeanFactoryAware {

	private static Log log = LogFactory.getLog(BindableProxyFactory.class);

	private final Map<Method, Object> targetCache = new HashMap<>(2);

	private Object proxy;

	protected BeanFactory beanFactory;

	public BindableProxyFactory(Class<?> type) {
		super(type);
		this.type = type;
	}

	@Override
	public synchronized Object invoke(MethodInvocation invocation) {
		Method method = invocation.getMethod();

		// try to use cached target
		Object boundTarget = this.targetCache.get(method);
		if (boundTarget != null) {
			return boundTarget;
		}

		Input input = AnnotationUtils.findAnnotation(method, Input.class);
		if (input != null) {
			String name = BindingBeanDefinitionRegistryUtils.getBindingTargetName(input,
					method);
			boundTarget = this.inputHolders.get(name).getBoundTarget();
			this.targetCache.put(method, boundTarget);
			return boundTarget;
		}
		else {
			Output output = AnnotationUtils.findAnnotation(method, Output.class);
			if (output != null) {
				String name = BindingBeanDefinitionRegistryUtils
						.getBindingTargetName(output, method);
				boundTarget = this.outputHolders.get(name).getBoundTarget();
				this.targetCache.put(method, boundTarget);
				return boundTarget;
			}
		}
		return null;
	}

	public void replaceInputChannel(String originalChannelName, String newChannelName, SubscribableChannel messageChannel) {
		if (log.isInfoEnabled()) {
			log.info("Replacing '" + originalChannelName + "' binding channel with '" + newChannelName + "'");
		}
		BoundTargetHolder holder = new BoundTargetHolder(messageChannel, true);
		this.inputHolders.remove(originalChannelName);
		this.inputHolders.put(newChannelName, holder);
	}

	public void replaceOutputChannel(String originalChannelName, String newChannelName, MessageChannel messageChannel) {
		if (log.isInfoEnabled()) {
			log.info("Replacing '" + originalChannelName + "' binding channel with '" + newChannelName + "'");
		}
		BoundTargetHolder holder = new BoundTargetHolder(messageChannel, true);
		this.outputHolders.remove(originalChannelName);
		this.outputHolders.put(newChannelName, holder);
	}

	@Override
	public void afterPropertiesSet() {
		populateBindingTargetFactories(this.beanFactory);
		Assert.notEmpty(BindableProxyFactory.this.bindingTargetFactories,
				"'bindingTargetFactories' cannot be empty");
		ReflectionUtils.doWithMethods(this.type, method -> {
			Input input = AnnotationUtils.findAnnotation(method, Input.class);
			if (input != null) {
				String name = BindingBeanDefinitionRegistryUtils
						.getBindingTargetName(input, method);
				Class<?> returnType = method.getReturnType();

				BindableProxyFactory.this.inputHolders.put(name,
						new BoundTargetHolder(getBindingTargetFactory(returnType)
								.createInput(name), true));
			}
		});
		ReflectionUtils.doWithMethods(this.type, method -> {
			Output output = AnnotationUtils.findAnnotation(method, Output.class);
			if (output != null) {
				String name = BindingBeanDefinitionRegistryUtils
						.getBindingTargetName(output, method);
				Class<?> returnType = method.getReturnType();

				BindableProxyFactory.this.outputHolders.put(name,
						new BoundTargetHolder(getBindingTargetFactory(returnType)
								.createOutput(name), true));
			}
		});
	}

	@Override
	public synchronized Object getObject() {
		if (this.proxy == null && this.type != null) {
			ProxyFactory factory = new ProxyFactory(this.type, this);
			this.proxy = factory.getProxy();
		}
		return this.proxy;
	}

	@Override
	public Class<?> getObjectType() {
		return this.type;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}
}
