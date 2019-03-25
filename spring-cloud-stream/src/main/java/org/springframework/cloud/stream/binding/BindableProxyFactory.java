/*
 * Copyright 2015-2018 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collection;
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
import org.springframework.cloud.stream.aggregate.SharedBindingTargetRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.internal.InternalPropertyNames;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link FactoryBean} for instantiating the interfaces specified via
 * {@link EnableBinding}.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @see EnableBinding
 */
public class BindableProxyFactory
		implements MethodInterceptor, FactoryBean<Object>, Bindable, InitializingBean {

	private static Log log = LogFactory.getLog(BindableProxyFactory.class);

	private final Map<Method, Object> targetCache = new HashMap<>(2);

	@Value("${" + InternalPropertyNames.NAMESPACE_PROPERTY_NAME + ":}")
	private String namespace;

	@Autowired(required = false)
	private SharedBindingTargetRegistry sharedBindingTargetRegistry;

	@Autowired
	private Map<String, BindingTargetFactory> bindingTargetFactories;

	private Class<?> type;

	private Object proxy;

	private Map<String, BoundTargetHolder> inputHolders = new HashMap<>();

	private Map<String, BoundTargetHolder> outputHolders = new HashMap<>();

	public BindableProxyFactory(Class<?> type) {
		this.type = type;
	}

	@Override
	public synchronized Object invoke(MethodInvocation invocation) throws Throwable {
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

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notEmpty(BindableProxyFactory.this.bindingTargetFactories,
				"'bindingTargetFactories' cannot be empty");
		ReflectionUtils.doWithMethods(this.type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException {
				Input input = AnnotationUtils.findAnnotation(method, Input.class);
				if (input != null) {
					String name = BindingBeanDefinitionRegistryUtils
							.getBindingTargetName(input, method);
					Class<?> returnType = method.getReturnType();
					Object sharedBindingTarget = locateSharedBindingTarget(name,
							returnType);
					if (sharedBindingTarget != null) {
						BindableProxyFactory.this.inputHolders.put(name,
								new BoundTargetHolder(sharedBindingTarget, false));
					}
					else {
						BindableProxyFactory.this.inputHolders.put(name,
								new BoundTargetHolder(getBindingTargetFactory(returnType)
										.createInput(name), true));
					}
				}
			}
		});
		ReflectionUtils.doWithMethods(this.type, new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(Method method) throws IllegalArgumentException {
				Output output = AnnotationUtils.findAnnotation(method, Output.class);
				if (output != null) {
					String name = BindingBeanDefinitionRegistryUtils
							.getBindingTargetName(output, method);
					Class<?> returnType = method.getReturnType();
					Object sharedBindingTarget = locateSharedBindingTarget(name,
							returnType);
					if (sharedBindingTarget != null) {
						BindableProxyFactory.this.outputHolders.put(name,
								new BoundTargetHolder(sharedBindingTarget, false));
					}
					else {
						BindableProxyFactory.this.outputHolders.put(name,
								new BoundTargetHolder(getBindingTargetFactory(returnType)
										.createOutput(name), true));
					}
				}
			}
		});
	}

	private BindingTargetFactory getBindingTargetFactory(Class<?> bindingTargetType) {
		List<String> candidateBindingTargetFactories = new ArrayList<>();
		for (Map.Entry<String, BindingTargetFactory> bindingTargetFactoryEntry : this.bindingTargetFactories
				.entrySet()) {
			if (bindingTargetFactoryEntry.getValue().canCreate(bindingTargetType)) {
				candidateBindingTargetFactories.add(bindingTargetFactoryEntry.getKey());
			}
		}
		if (candidateBindingTargetFactories.size() == 1) {
			return this.bindingTargetFactories
					.get(candidateBindingTargetFactories.get(0));
		}
		else {
			if (candidateBindingTargetFactories.size() == 0) {
				throw new IllegalStateException(
						"No factory found for binding target type: "
								+ bindingTargetType.getName()
								+ " among registered factories: "
								+ StringUtils.collectionToCommaDelimitedString(
										this.bindingTargetFactories.keySet()));
			}
			else {
				throw new IllegalStateException(
						"Multiple factories found for binding target type: "
								+ bindingTargetType.getName() + ": "
								+ StringUtils.collectionToCommaDelimitedString(
										candidateBindingTargetFactories));
			}
		}
	}

	private <T> T locateSharedBindingTarget(String name, Class<T> bindingTargetType) {
		return this.sharedBindingTargetRegistry != null
				? this.sharedBindingTargetRegistry.get(
						getNamespacePrefixedBindingTargetName(name), bindingTargetType)
				: null;
	}

	private String getNamespacePrefixedBindingTargetName(String name) {
		return this.namespace + "." + name;
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

	/**
	 * @deprecated in favor of {@link #createAndBindInputs(BindingService)}
	 */
	@Override
	@Deprecated
	public void bindInputs(BindingService bindingService) {
		this.createAndBindInputs(bindingService);
	}

	@Override
	public Collection<Binding<Object>> createAndBindInputs(
			BindingService bindingService) {
		List<Binding<Object>> bindings = new ArrayList<>();
		if (log.isDebugEnabled()) {
			log.debug(
					String.format("Binding inputs for %s:%s", this.namespace, this.type));
		}
		for (Map.Entry<String, BoundTargetHolder> boundTargetHolderEntry : this.inputHolders
				.entrySet()) {
			String inputTargetName = boundTargetHolderEntry.getKey();
			BoundTargetHolder boundTargetHolder = boundTargetHolderEntry.getValue();
			if (boundTargetHolder.isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.namespace, this.type,
							inputTargetName));
				}
				bindings.addAll(bindingService.bindConsumer(
						boundTargetHolder.getBoundTarget(), inputTargetName));
			}
		}
		return bindings;
	}

	/**
	 * @deprecated in favor of {@link #createAndBindOutputs(BindingService)}
	 */
	@Override
	@Deprecated
	public void bindOutputs(BindingService bindingService) {
		this.createAndBindOutputs(bindingService);
	}

	@Override
	public Collection<Binding<Object>> createAndBindOutputs(
			BindingService bindingService) {
		List<Binding<Object>> bindings = new ArrayList<>();
		if (log.isDebugEnabled()) {
			log.debug(String.format("Binding outputs for %s:%s", this.namespace,
					this.type));
		}
		for (Map.Entry<String, BoundTargetHolder> boundTargetHolderEntry : this.outputHolders
				.entrySet()) {
			BoundTargetHolder boundTargetHolder = boundTargetHolderEntry.getValue();
			String outputTargetName = boundTargetHolderEntry.getKey();
			if (boundTargetHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.namespace, this.type,
							outputTargetName));
				}
				bindings.add(bindingService.bindProducer(
						boundTargetHolder.getBoundTarget(), outputTargetName));
			}
		}
		return bindings;
	}

	@Override
	public void unbindInputs(BindingService bindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Unbinding inputs for %s:%s", this.namespace,
					this.type));
		}
		for (Map.Entry<String, BoundTargetHolder> boundTargetHolderEntry : this.inputHolders
				.entrySet()) {
			if (boundTargetHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Unbinding %s:%s:%s", this.namespace,
							this.type, boundTargetHolderEntry.getKey()));
				}
				bindingService.unbindConsumers(boundTargetHolderEntry.getKey());
			}
		}
	}

	@Override
	public void unbindOutputs(BindingService bindingService) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Unbinding outputs for %s:%s", this.namespace,
					this.type));
		}
		for (Map.Entry<String, BoundTargetHolder> boundTargetHolderEntry : this.outputHolders
				.entrySet()) {
			if (boundTargetHolderEntry.getValue().isBindable()) {
				if (log.isDebugEnabled()) {
					log.debug(String.format("Binding %s:%s:%s", this.namespace, this.type,
							boundTargetHolderEntry.getKey()));
				}
				bindingService.unbindProducers(boundTargetHolderEntry.getKey());
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
	 * Holds information about the binding targets exposed by the interface proxy, as well
	 * as their status.
	 */
	private final class BoundTargetHolder {

		private Object boundTarget;

		private boolean bindable;

		private BoundTargetHolder(Object boundTarget, boolean bindable) {
			this.boundTarget = boundTarget;
			this.bindable = bindable;
		}

		public Object getBoundTarget() {
			return this.boundTarget;
		}

		public boolean isBindable() {
			return this.bindable;
		}

	}

}
