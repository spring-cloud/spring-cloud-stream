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

package org.springframework.cloud.stream.config;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.util.Assert;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 */
@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
public class BindingServiceProperties
		implements ApplicationContextAware, InitializingBean {

	private static final int DEFAULT_BINDING_RETRY_INTERVAL = 30;

	/**
	 * The instance id of the application: a number from 0 to instanceCount-1. Used for
	 * partitioning and with Kafka. NOTE: Could also be managed per individual binding
	 * "spring.cloud.stream.bindings.foo.consumer.instance-index" where 'foo' is the name
	 * of the binding.
	 */
	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex;

	/**
	 * The number of deployed instances of an application. Default: 1. NOTE: Could also be
	 * managed per individual binding
	 * "spring.cloud.stream.bindings.foo.consumer.instance-count" where 'foo' is the name
	 * of the binding.
	 */
	private int instanceCount = 1;

	/**
	 * Additional binding properties (see {@link BinderProperties}) per binding name
	 * (e.g., 'input`).
	 *
	 * For example; This sets the content-type for the 'input' binding of a Sink
	 * application: 'spring.cloud.stream.bindings.input.contentType=text/plain'
	 */
	private Map<String, BindingProperties> bindings = new TreeMap<>(
			String.CASE_INSENSITIVE_ORDER);

	/**
	 * Additional per-binder properties (see {@link BinderProperties}) if more then one
	 * binder of the same type is used (i.e., connect to multiple instances of RabbitMq).
	 * Here you can specify multiple binder configurations, each with different
	 * environment settings. For example; spring.cloud.stream.binders.rabbit1.environment.
	 * . . , spring.cloud.stream.binders.rabbit2.environment. . .
	 */
	private Map<String, BinderProperties> binders = new HashMap<>();

	/**
	 * The name of the binder to use by all bindings in the event multiple binders
	 * available (e.g., 'rabbit').
	 */
	private String defaultBinder;

	/**
	 * A list of destinations that can be bound dynamically. If set, only listed
	 * destinations can be bound.
	 */
	private String[] dynamicDestinations = new String[0];

	/**
	 * Retry interval (in seconds) used to schedule binding attempts. Default: 30 sec.
	 */
	private int bindingRetryInterval = DEFAULT_BINDING_RETRY_INTERVAL;

	private ConfigurableApplicationContext applicationContext = new GenericApplicationContext();

	private ConversionService conversionService;

	public Map<String, BindingProperties> getBindings() {
		return this.bindings;
	}

	public void setBindings(Map<String, BindingProperties> bindings) {
		this.bindings.putAll(bindings);
	}

	public Map<String, BinderProperties> getBinders() {
		return this.binders;
	}

	public void setBinders(Map<String, BinderProperties> binders) {
		this.binders = binders;
	}

	public String getDefaultBinder() {
		return this.defaultBinder;
	}

	public void setDefaultBinder(String defaultBinder) {
		this.defaultBinder = defaultBinder;
	}

	public int getInstanceIndex() {
		return this.instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public int getInstanceCount() {
		return this.instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	public String[] getDynamicDestinations() {
		return this.dynamicDestinations;
	}

	public void setDynamicDestinations(String[] dynamicDestinations) {
		this.dynamicDestinations = dynamicDestinations;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		GenericConversionService cs = (GenericConversionService) IntegrationUtils
				.getConversionService(this.applicationContext.getBeanFactory());
		if (this.applicationContext.containsBean("spelConverter")) {
			Converter<?, ?> converter = (Converter<?, ?>) this.applicationContext
					.getBean("spelConverter");
			cs.addConverter(converter);
		}
	}

	public void setConversionService(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.conversionService == null) {
			this.conversionService = this.applicationContext.getBean(
					IntegrationUtils.INTEGRATION_CONVERSION_SERVICE_BEAN_NAME,
					ConversionService.class);
		}
	}

	public String getBinder(String bindingName) {
		return getBindingProperties(bindingName).getBinder();
	}

	/**
	 * Return configuration properties as Map.
	 * @return map of binding configuration properties.
	 */
	public Map<String, Object> asMapProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put("instanceIndex", String.valueOf(getInstanceIndex()));
		properties.put("instanceCount", String.valueOf(getInstanceCount()));
		properties.put("defaultBinder", getDefaultBinder());
		properties.put("dynamicDestinations", getDynamicDestinations());
		for (Map.Entry<String, BindingProperties> entry : this.bindings.entrySet()) {
			properties.put(entry.getKey(), entry.getValue().toString());
		}
		for (Map.Entry<String, BinderProperties> entry : this.binders.entrySet()) {
			properties.put(entry.getKey(), entry.getValue());
		}
		return properties;
	}

	public ConsumerProperties getConsumerProperties(String inputBindingName) {
		Assert.notNull(inputBindingName, "The input binding name cannot be null");
		BindingProperties bindingProperties = getBindingProperties(inputBindingName);
		ConsumerProperties consumerProperties = bindingProperties.getConsumer();
		if (consumerProperties == null) {
			consumerProperties = new ConsumerProperties();
			bindingProperties.setConsumer(consumerProperties);
		}
		// propagate instance count and instance index if not already set
		if (consumerProperties.getInstanceCount() < 0) {
			consumerProperties.setInstanceCount(this.instanceCount);
		}
		if (consumerProperties.getInstanceIndex() < 0) {
			consumerProperties.setInstanceIndex(this.instanceIndex);
		}
		return consumerProperties;
	}

	public ProducerProperties getProducerProperties(String outputBindingName) {
		Assert.notNull(outputBindingName, "The output binding name cannot be null");
		BindingProperties bindingProperties = getBindingProperties(outputBindingName);
		ProducerProperties producerProperties = bindingProperties.getProducer();
		if (producerProperties == null) {
			producerProperties = new ProducerProperties();
			bindingProperties.setProducer(producerProperties);
		}
		return producerProperties;
	}

	public BindingProperties getBindingProperties(String bindingName) {
		this.bindIfNecessary(bindingName);
		BindingProperties bindingProperties = this.bindings.get(bindingName);
		if (bindingProperties.getDestination() == null) {
			bindingProperties.setDestination(bindingName);
		}
		return bindingProperties;
	}

	public String getGroup(String bindingName) {
		return getBindingProperties(bindingName).getGroup();
	}

	public String getBindingDestination(String bindingName) {
		return getBindingProperties(bindingName).getDestination();
	}

	public int getBindingRetryInterval() {
		return this.bindingRetryInterval;
	}

	public void setBindingRetryInterval(int bindingRetryInterval) {
		this.bindingRetryInterval = bindingRetryInterval;
	}

	public void updateProducerProperties(String bindingName,
			ProducerProperties producerProperties) {
		if (this.bindings.containsKey(bindingName)) {
			this.bindings.get(bindingName).setProducer(producerProperties);
		}
	}

	/*
	 * The "necessary" implies the scenario where only defaults are defined.
	 */
	private void bindIfNecessary(String bindingName) {
		if (!this.bindings.containsKey(bindingName)) {
			this.bindToDefault(bindingName);
		}
	}

	private void bindToDefault(String binding) {
		BindingProperties bindingPropertiesTarget = new BindingProperties();
		Binder binder = new Binder(
				ConfigurationPropertySources
						.get(this.applicationContext.getEnvironment()),
				new PropertySourcesPlaceholdersResolver(
						this.applicationContext.getEnvironment()),
				IntegrationUtils.getConversionService(
						this.applicationContext.getBeanFactory()),
				null);
		binder.bind("spring.cloud.stream.default",
				Bindable.ofInstance(bindingPropertiesTarget));
		this.bindings.put(binding, bindingPropertiesTarget);
	}

}
