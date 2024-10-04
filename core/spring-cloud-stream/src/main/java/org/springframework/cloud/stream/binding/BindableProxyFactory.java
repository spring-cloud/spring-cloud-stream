/*
 * Copyright 2015-2024 the original author or authors.
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
import java.util.concurrent.locks.ReentrantLock;

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
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for instantiating the functions.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author Omer Celik
 */
public class BindableProxyFactory extends AbstractBindableProxyFactory
		implements MethodInterceptor, FactoryBean<Object>, InitializingBean, BeanFactoryAware {

	private static Log log = LogFactory.getLog(BindableProxyFactory.class);

	private final Map<Method, Object> targetCache = new HashMap<>(2);

	private Object proxy;

	protected BeanFactory beanFactory;

	private final ReentrantLock lock = new ReentrantLock();

	public BindableProxyFactory(Class<?> type) {
		super(type);
		this.type = type;
	}

	@Override
	public Object invoke(MethodInvocation invocation) {
		try {
			lock.lock();
			Method method = invocation.getMethod();

			// try to use cached target
            return this.targetCache.get(method);
        }
		finally {
			lock.unlock();
		}
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
	}

	@Override
	public Object getObject() {
		try {
			lock.lock();
			if (this.proxy == null && this.type != null) {
				ProxyFactory factory = new ProxyFactory(this.type, this);
				this.proxy = factory.getProxy();
			}
			return this.proxy;
		}
		finally {
			lock.unlock();
		}
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
