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

package org.springframework.cloud.stream.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
public class ChannelBindingServiceProperties implements ApplicationContextAware, InitializingBean {

	private ConversionService conversionService;

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex = 0;

	private int instanceCount = 1;

	private Map<String, BindingProperties> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private Map<String, BinderProperties> binders = new HashMap<>();

	private Properties consumerDefaults = new Properties();

	private Properties producerDefaults = new Properties();

	private String defaultBinder;

	private String[] dynamicDestinations = new String[0];

	private boolean ignoreUnknownProperties = true;

	private boolean autoStartContext = true;

	private ConfigurableApplicationContext applicationContext;

	public Map<String, BindingProperties> getBindings() {
		return bindings;
	}

	public void setBindings(Map<String, BindingProperties> bindings) {
		this.bindings = bindings;
	}

	public Map<String, BinderProperties> getBinders() {
		return binders;
	}

	public void setBinders(Map<String, BinderProperties> binders) {
		this.binders = binders;
	}

	public String getDefaultBinder() {
		return defaultBinder;
	}

	public void setDefaultBinder(String defaultBinder) {
		this.defaultBinder = defaultBinder;
	}

	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public int getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	public String[] getDynamicDestinations() {
		return dynamicDestinations;
	}

	public void setDynamicDestinations(String[] dynamicDestinations) {
		this.dynamicDestinations = dynamicDestinations;
	}

	public Properties getConsumerDefaults() {
		return consumerDefaults;
	}

	public void setConsumerDefaults(Properties consumerDefaults) {
		this.consumerDefaults = consumerDefaults;
	}

	public Properties getProducerDefaults() {
		return producerDefaults;
	}

	public void setProducerDefaults(Properties producerDefaults) {
		this.producerDefaults = producerDefaults;
	}

	public boolean isIgnoreUnknownProperties() {
		return ignoreUnknownProperties;
	}

	public void setIgnoreUnknownProperties(boolean ignoreUnknownProperties) {
		this.ignoreUnknownProperties = ignoreUnknownProperties;
	}

	public boolean isAutoStartContext() {
		return this.autoStartContext;
	}

	public void setAutoStartContext(boolean autoStartContext) {
		this.autoStartContext = autoStartContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (conversionService == null) {
			conversionService = applicationContext.getBean(IntegrationUtils.INTEGRATION_CONVERSION_SERVICE_BEAN_NAME, ConversionService.class);
		}
	}

	public String getBinder(String channelName) {
		return getBindingProperties(channelName).getBinder();
	}

	/**
	 * Return configuration properties as Map.
	 * @return map of channel binding configuration properties.
	 */
	public Map<String, Object> asMapProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put("instanceIndex", String.valueOf(getInstanceIndex()));
		properties.put("instanceCount", String.valueOf(getInstanceCount()));
		properties.put("defaultBinder", getDefaultBinder());
		properties.put("dynamicDestinations", getDynamicDestinations());
		for (Map.Entry<String, BindingProperties> entry : bindings.entrySet()) {
			properties.put(entry.getKey(), entry.getValue().toString());
		}
		for (Map.Entry<String, BinderProperties> entry : binders.entrySet()) {
			properties.put(entry.getKey(), entry.getValue());
		}
		return properties;
	}

	public ConsumerProperties getConsumerProperties(String inputChannelName) {
		Assert.notNull(inputChannelName, "The input channel name cannot be null");
		ConsumerProperties consumerProperties = getBindingProperties(inputChannelName).getConsumer();
		if (consumerProperties == null) {
			consumerProperties = new ConsumerProperties();
		}
		consumerProperties.setInstanceCount(this.instanceCount);
		consumerProperties.setInstanceIndex(this.instanceIndex);
		return consumerProperties;
	}

	public ProducerProperties getProducerProperties(String outputChannelName) {
		Assert.notNull(outputChannelName, "The output channel name cannot be null");
		ProducerProperties producerProperties = getBindingProperties(outputChannelName).getProducer();
		if (producerProperties == null) {
			producerProperties = new ProducerProperties();
		}
		return producerProperties;
	}

	public BindingProperties getBindingProperties(String channelName) {
		BindingProperties bindingProperties = new BindingProperties();
		if (this.bindings.containsKey(channelName)) {
			BeanUtils.copyProperties(this.bindings.get(channelName), bindingProperties);
		}
		if (bindingProperties.getDestination() == null) {
			bindingProperties.setDestination(channelName);
		}
		return bindingProperties;
	}

	public String getGroup(String channelName) {
		return getBindingProperties(channelName).getGroup();
	}

	public String getBindingDestination(String channelName) {
		return getBindingProperties(channelName).getDestination();
	}
}
