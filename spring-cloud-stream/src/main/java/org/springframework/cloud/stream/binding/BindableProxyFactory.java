/*
 * Copyright 2015-2016 the original author or authors.
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.aggregate.SharedChannelRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * {@link FactoryBean} for instantiating the interfaces specified via
 * {@link EnableBinding}
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 *
 * @see EnableBinding
 */
public class BindableProxyFactory implements MethodInterceptor, FactoryBean<Object>, Bindable, InitializingBean {

	private static Log log = LogFactory.getLog(BindableProxyFactory.class);

	private static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	private static final String CHANNEL_NAMESPACE_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX
			+ ".channelNamespace";

	@Value("${" + CHANNEL_NAMESPACE_PROPERTY_NAME + ":}")
	private String channelNamespace;

	@Autowired(required = false)
	private SharedChannelRegistry sharedChannelRegistry;

	@Autowired
	private List<BoundElementFactory> boundElementFactories;

	private Class<?> type;

	private Object proxy;

	private Map<String, BoundElementHolder> inputHolders = new HashMap<>();

	private Map<String, BoundElementHolder> outputHolders = new HashMap<>();

	public BindableProxyFactory(Class<?> type) {
		this.type = type;
	}

	@Override
	public synchronized Object invoke(MethodInvocation invocation) throws Throwable {
		Method method = invocation.getMethod();
		Input input = AnnotationUtils.findAnnotation(method, Input.class);
		if (input != null) {
			String name = BindingBeanDefinitionRegistryUtils.getChannelName(input, method);
			return this.inputHolders.get(name).getBoundElement();
		}
		else {
			Output output = AnnotationUtils.findAnnotation(method, Output.class);
			if (output != null) {
				String name = BindingBeanDefinitionRegistryUtils.getChannelName(output, method);
				return this.outputHolders.get(name).getBoundElement();
			}
		}
		return null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(BindableProxyFactory.this.boundElementFactories, "'boundElementFactories' cannot be null");
		Assert.notEmpty(BindableProxyFactory.this.boundElementFactories, "'boundElementFactories' cannot be empty");
		ReflectionUtils.doWithMethods(this.type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException {
				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = BindingBeanDefinitionRegistryUtils.getChannelName(input, method);
					Class<?> returnType = method.getReturnType();
					Object sharedBoundElement = locateSharedBoundElement(name, returnType);
					if (sharedBoundElement != null) {
						BindableProxyFactory.this.inputHolders.put(name,
								new BoundElementHolder(sharedBoundElement, false));
					}
					else {
						BindableProxyFactory.this.inputHolders.put(name,
								new BoundElementHolder(getBoundElementFactory(returnType).createInput(name), true));
					}
				}
			}
		});
		ReflectionUtils.doWithMethods(this.type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException {
				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = BindingBeanDefinitionRegistryUtils.getChannelName(output, method);
					Class<?> returnType = method.getReturnType();
					Object sharedBoundElement = locateSharedBoundElement(name, returnType);
					if (sharedBoundElement != null) {
						BindableProxyFactory.this.outputHolders.put(name,
								new BoundElementHolder(sharedBoundElement, false));
					}
					else {
						BindableProxyFactory.this.outputHolders.put(name,
								new BoundElementHolder(getBoundElementFactory(returnType).createOutput(name), true));
					}
				}
			}
		});
	}

	private BoundElementFactory getBoundElementFactory(Class<?> boundElementType) {
		for (BoundElementFactory factory : boundElementFactories) {
			if (factory.canCreate(boundElementType)) {
				return factory;
			}
		}
		throw new IllegalStateException("No factory found for bound element type: " + boundElementType.getName());
	}

	private MessageChannel locateSharedBoundElement(String name, Class<?> boundElementType) {
		return MessageChannel.class.isAssignableFrom(boundElementType) && this.sharedChannelRegistry != null
				? this.sharedChannelRegistry.get(getNamespacePrefixedChannelName(name)) : null;
	}

	private String getNamespacePrefixedChannelName(String name) {
		return this.channelNamespace + "." + name;
	}

	@Override
	public synchronized Object getObject() throws Exception {
		if (this.proxy == null) {
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
	public void bindInputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Binding inputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, BoundElementHolder> channelHolderEntry : this.inputHolders.entrySet()) {
			String inputChannelName = channelHolderEntry.getKey();
			BoundElementHolder boundElementHolder = channelHolderEntry.getValue();
			if (boundElementHolder.isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.channelNamespace, this.type, inputChannelName));
				}
				channelBindingService.bindConsumer(boundElementHolder.getBoundElement(), inputChannelName);
			}
		}
	}

	@Override
	public void bindOutputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Binding outputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, BoundElementHolder> channelHolderEntry : this.outputHolders.entrySet()) {
			BoundElementHolder boundElementHolder = channelHolderEntry.getValue();
			String outputChannelName = channelHolderEntry.getKey();
			if (channelHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.channelNamespace, this.type, outputChannelName));
				}
				channelBindingService.bindProducer(boundElementHolder.getBoundElement(), outputChannelName);
			}
		}
	}

	@Override
	public void unbindInputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Unbinding inputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, BoundElementHolder> channelHolderEntry : this.inputHolders.entrySet()) {
			if (channelHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Unbinding %s:%s:%s", this.channelNamespace, this.type,
							channelHolderEntry.getKey()));
				}
				channelBindingService.unbindConsumers(channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public void unbindOutputs(ChannelBindingService channelBindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Unbinding outputs for %s:%s", this.channelNamespace, this.type));
		}
		for (Map.Entry<String, BoundElementHolder> channelHolderEntry : this.outputHolders.entrySet()) {
			if (channelHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.channelNamespace, this.type,
							channelHolderEntry.getKey()));
				}
				channelBindingService.unbindProducers(channelHolderEntry.getKey());
			}
		}
	}

	@Override
	public Set<String> getInputs() {
		return this.inputHolders.keySet();
	}

	@Override
	public Set<String> getOutputs() {
		return this.outputHolders.keySet();
	}

	/**
	 * Holds information about the channels exposed by the interface proxy, as well as
	 * their status.
	 */
	private final class BoundElementHolder {

		private Object boundElement;

		private boolean bindable;

		private BoundElementHolder(Object boundElement, boolean bindable) {
			this.boundElement = boundElement;
			this.bindable = bindable;
		}

		public Object getBoundElement() {
			return this.boundElement;
		}

		public boolean isBindable() {
			return this.bindable;
		}
	}
}
