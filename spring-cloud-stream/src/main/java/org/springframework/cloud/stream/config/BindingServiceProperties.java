/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.util.Assert;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
public class BindingServiceProperties implements ApplicationContextAware, InitializingBean {

	private static final int DEFAULT_BINDING_RETRY_INTERVAL = 30;

	private ConversionService conversionService;

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex;

	private int instanceCount = 1;

	private Map<String, BindingProperties> bindings = new TreeMap<>(
			String.CASE_INSENSITIVE_ORDER);

	private Map<String, BinderProperties> binders = new HashMap<>();

	private String defaultBinder;

	private String[] dynamicDestinations = new String[0];

	private ConfigurableApplicationContext applicationContext;

	private int bindingRetryInterval = DEFAULT_BINDING_RETRY_INTERVAL;

	public Map<String, BindingProperties> getBindings() {
		return this.bindings;
	}

	public void setBindings(Map<String, BindingProperties> bindings) {
		this.bindings = bindings;
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
		if (this.applicationContext.getEnvironment() instanceof ConfigurableEnvironment) {
			// override the bindings store with the environment-initializing version if in
			// a Spring context
			Map<String, BindingProperties> delegate = new TreeMap<String, BindingProperties>(
					String.CASE_INSENSITIVE_ORDER);
			delegate.putAll(this.bindings);
			this.bindings = new EnvironmentEntryInitializingTreeMap<>(this.applicationContext.getEnvironment(),
					BindingProperties.class, "spring.cloud.stream.default", delegate,
					IntegrationUtils.getConversionService(this.applicationContext.getBeanFactory()));
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
		BindingProperties bindingProperties = new BindingProperties();
		if (this.bindings.containsKey(bindingName)) {
			BeanUtils.copyProperties(this.bindings.get(bindingName), bindingProperties);
		}
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

	public void updateProducerProperties(String bindingName, ProducerProperties producerProperties) {
		if (this.bindings.containsKey(bindingName)) {
			this.bindings.get(bindingName).setProducer(producerProperties);
		}
	}

}
