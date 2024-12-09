/*
 * Copyright 2018-2022 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.validation.MessageInterpolatorFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.util.ClassUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
/**
 * Base implementation of {@link ExtendedBindingProperties}.
 *
 * @param <C> - consumer properties type
 * @param <P> - producer properties type
 * @param <T> - type which provides the consumer and producer properties
 * @author Oleg Zhurakousky
 * @since 2.1
 */
@Validated
public abstract class AbstractExtendedBindingProperties<C, P, T extends BinderSpecificPropertiesProvider>
		implements ExtendedBindingProperties<C, P>, ApplicationContextAware {

	static Log logger = LogFactory.getLog(AbstractExtendedBindingProperties.class);

	private final Map<String, T> bindings = new HashMap<>();

	private ConfigurableApplicationContext applicationContext = new GenericApplicationContext();

	public void setBindings(Map<String, T> bindings) {
		this.bindings.putAll(bindings);
	}

	@SuppressWarnings("unchecked")
	@Override
	public C getExtendedConsumerProperties(String binding) {
		this.bindIfNecessary(binding);
		return (C) this.bindings.get(binding).getConsumer();
	}

	@SuppressWarnings("unchecked")
	@Override
	public P getExtendedProducerProperties(String binding) {
		this.bindIfNecessary(binding);
		return (P) this.bindings.get(binding).getProducer();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	/*
	 * The "necessary" implies the scenario where only defaults are defined.
	 */
	private void bindIfNecessary(String bindingName) {
		if (!this.bindings.containsKey(bindingName)) {
			this.bindToDefault(bindingName);
		}
	}

	@SuppressWarnings("unchecked")
	private void bindToDefault(String binding) {
		T extendedBindingPropertiesTarget = (T) BeanUtils
				.instantiateClass(this.getExtendedPropertiesEntryClass());

		GenericConversionService cs = (GenericConversionService) this.applicationContext.getBeanFactory().getConversionService();

		Iterable<ConfigurationPropertySource> sources = ConfigurationPropertySources.get(this.applicationContext.getEnvironment());
		PropertySourcesPlaceholdersResolver placeholdersResolver = new PropertySourcesPlaceholdersResolver(this.applicationContext.getEnvironment());
		Binder binder =  new Binder(sources, placeholdersResolver, cs, null, null);

		if (Jsr303Validator.isJsr303Present(this.applicationContext)) {
			Jsr303Validator validator = new Jsr303Validator(this.applicationContext);
			binder.bind(this.getDefaultsPrefix(),
					Bindable.ofInstance(extendedBindingPropertiesTarget), new ValidationBindHandler(validator));
		}
		else {
			binder.bind(this.getDefaultsPrefix(),
					Bindable.ofInstance(extendedBindingPropertiesTarget));
		}
		this.bindings.put(binding, extendedBindingPropertiesTarget);
	}

	protected Map<String, T> doGetBindings() {
		return Collections.unmodifiableMap(this.bindings);
	}

	private class Jsr303Validator implements Validator {

		private static final String[] VALIDATOR_CLASSES = { "jakarta.validation.Validator",
				"jakarta.validation.ValidatorFactory", "jakarta.validation.bootstrap.GenericBootstrap" };

		private final Delegate delegate;

		Jsr303Validator(ApplicationContext applicationContext) {
			this.delegate = new Delegate(applicationContext);
		}

		@Override
		public boolean supports(Class<?> type) {
			return this.delegate.supports(type);
		}

		@Override
		public void validate(Object target, Errors errors) {
			this.delegate.validate(target, errors);
		}

		static boolean isJsr303Present(ApplicationContext applicationContext) {
			ClassLoader classLoader = applicationContext.getClassLoader();
			for (String validatorClass : VALIDATOR_CLASSES) {
				if (!ClassUtils.isPresent(validatorClass, classLoader)) {
					return false;
				}
			}
			return true;
		}

		private static class Delegate extends LocalValidatorFactoryBean {

			Delegate(ApplicationContext applicationContext) {
				setApplicationContext(applicationContext);
				setMessageInterpolator(new MessageInterpolatorFactory(applicationContext).getObject());
				try {
					afterPropertiesSet();
				}
				catch (Exception e) {
					logger.warn("Failed to execute afterPropertiesSet() on aplication context", e);
				}
			}

		}
	}
}
