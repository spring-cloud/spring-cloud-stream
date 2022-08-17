/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.lang.model.element.Modifier;

import org.apache.commons.logging.LogFactory;

import org.springframework.aot.generate.GeneratedMethod;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.MethodReference;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.aot.BeanRegistrationCode;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.aot.ApplicationContextAotGenerator;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.log.LogAccessor;
import org.springframework.javapoet.ClassName;
import org.springframework.util.Assert;

/**
 * @author Chris Bono
 * @since 4.0
 */
public class BinderChildContextInitializer implements ApplicationContextAware, BeanRegistrationAotProcessor {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));
	private DefaultBinderFactory binderFactory;
	private final Map<String, ApplicationContextInitializer<ConfigurableApplicationContext>> childContextInitializers;
	private volatile ConfigurableApplicationContext context;

	public BinderChildContextInitializer() {
		this.childContextInitializers = new HashMap<>();
	}

	public BinderChildContextInitializer(
			Map<String, ApplicationContextInitializer<ConfigurableApplicationContext>> childContextInitializers) {
		this.childContextInitializers = childContextInitializers;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		Assert.isInstanceOf(ConfigurableApplicationContext.class, applicationContext);
		this.context = (ConfigurableApplicationContext) applicationContext;
	}

	public void setBinderFactory(DefaultBinderFactory binderFactory) {
		Assert.notNull(binderFactory, () -> "binderFactory must be non-null");
		this.binderFactory = binderFactory;
		if (!this.childContextInitializers.isEmpty()) {
			this.logger.debug(() -> "Setting binder child context initializers on binder factory");
			this.binderFactory.setBinderChildContextInitializers(this.childContextInitializers);
		}
	}

	@Override
	public boolean isBeanExcludedFromAotProcessing() {
		return false;
	}

	@Override
	public BeanRegistrationAotContribution processAheadOfTime(RegisteredBean registeredBean) {
		if (registeredBean.getBeanClass().equals(getClass())) { //&& registeredBean.getBeanFactory().equals(this.context)) {
			this.logger.debug(() -> "Beginning AOT processing for binder child contexts");
			ensureBinderFactoryIsSet();
			Map<String, BinderConfiguration> binderConfigurations = this.binderFactory.getBinderConfigurations();
			Map<String, ConfigurableApplicationContext> binderChildContexts = binderConfigurations.entrySet().stream()
					.map(e -> Map.entry(e.getKey(), binderFactory.createBinderContextForAOT(e.getKey())))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			return new BinderChildContextAotContribution(binderChildContexts);
		}
		return null;
	}

	private void ensureBinderFactoryIsSet() {
		if (this.binderFactory == null) {
			Assert.notNull(this.context, () -> "Unable to lookup binder factory from context as this.context is null");
			this.binderFactory = this.context.getBean(DefaultBinderFactory.class);
		}
	}

	/**
	 * Callback for AOT generated {@link BinderChildContextAotContribution#applyTo(GenerationContext, BeanRegistrationCode)
	 * post-process method} which basically swaps the instance with one that uses the AOT generated child context
	 * initializers.
	 * @param childContextInitializers the child context initializers to use
	 * @return copy of this instance that uses the AOT generated child context initializers
	 */
	@SuppressWarnings({"unused", "raw"})
	public BinderChildContextInitializer withChildContextInitializers(
			Map<String, ApplicationContextInitializer<? extends ConfigurableApplicationContext>> childContextInitializers) {
		this.logger.debug(() -> "Replacing instance w/ one that uses; child context initializers");
		Map<String, ApplicationContextInitializer<ConfigurableApplicationContext>> downcastedInitializers =
				childContextInitializers.entrySet().stream()
						.map(e -> Map.entry(e.getKey(), (ApplicationContextInitializer<ConfigurableApplicationContext>) e.getValue()))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		return new BinderChildContextInitializer(downcastedInitializers);
	}

	/**
	 * An AOT contribution that generates a application context initializers that can be used at runtime to populate
	 * the child binder contexts.
	 */
	private static class BinderChildContextAotContribution implements BeanRegistrationAotContribution {

		private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));
		private final Map<String, GenericApplicationContext> childContexts;

		BinderChildContextAotContribution(Map<String, ConfigurableApplicationContext> childContexts) {
			this.childContexts = childContexts.entrySet().stream()
					.filter(e -> GenericApplicationContext.class.isInstance(e.getValue()))
					.map(e -> Map.entry(e.getKey(), GenericApplicationContext.class.cast(e.getValue())))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		}

		@Override
		public void applyTo(GenerationContext generationContext, BeanRegistrationCode beanRegistrationCode) {
			ApplicationContextAotGenerator aotGenerator = new ApplicationContextAotGenerator();

			GeneratedMethod postProcessorMethod = beanRegistrationCode.getMethods().add("addChildContextInitializers",
					(method) -> {
						method.addJavadoc("Use AOT child context initialization");
						method.addModifiers(Modifier.PRIVATE, Modifier.STATIC);
						method.addParameter(RegisteredBean.class, "registeredBean");
						method.addParameter(BinderChildContextInitializer.class, "instance");
						method.returns(BinderChildContextInitializer.class);
						method.addStatement("$T<String, $T<? extends $T>> initializers = new $T<>()", Map.class,
								ApplicationContextInitializer.class, ConfigurableApplicationContext.class, HashMap.class);
						this.childContexts.forEach((name, context) -> {
							this.logger.debug(() -> "Generating AOT child context initializer for " + name);
							GenerationContext childGenerationContext = generationContext.withName(name + "Binder");
							ClassName initializerClassName = aotGenerator.processAheadOfTime(context, childGenerationContext);
							method.addStatement("$T<? extends $T> " + name + "Initializer = new $L()", ApplicationContextInitializer.class,
									ConfigurableApplicationContext.class, initializerClassName);
							method.addStatement("initializers.put($S," + name + "Initializer)", name);
						});
						method.addStatement("return instance.withChildContextInitializers(initializers)");
					});
			beanRegistrationCode.addInstancePostProcessor(
					MethodReference.ofStatic(beanRegistrationCode.getClassName(), postProcessorMethod.getName()));
		}
	}

}
