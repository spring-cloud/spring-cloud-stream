package org.springframework.cloud.stream.config;

import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.ConversionService;

/**
 * Provides a wrapper around {@link BindingServiceProperties} for backwards compatibility.
 * @author Marius Bogoevici
 */
@Deprecated
public class ChannelBindingServiceProperties {

	private final BindingServiceProperties bindingServiceProperties;

	public ChannelBindingServiceProperties(
			BindingServiceProperties bindingServiceProperties) {
		this.bindingServiceProperties = bindingServiceProperties;
	}

	public Map<String, BindingProperties> getBindings() {
		return bindingServiceProperties.getBindings();
	}

	public void setBindings(Map<String, BindingProperties> bindings) {
		bindingServiceProperties.setBindings(bindings);
	}

	public Map<String, BinderProperties> getBinders() {
		return bindingServiceProperties.getBinders();
	}

	public void setBinders(Map<String, BinderProperties> binders) {
		bindingServiceProperties.setBinders(binders);
	}

	public String getDefaultBinder() {
		return bindingServiceProperties.getDefaultBinder();
	}

	public void setDefaultBinder(String defaultBinder) {
		bindingServiceProperties.setDefaultBinder(defaultBinder);
	}

	public int getInstanceIndex() {
		return bindingServiceProperties.getInstanceIndex();
	}

	public void setInstanceIndex(int instanceIndex) {
		bindingServiceProperties.setInstanceIndex(instanceIndex);
	}

	public int getInstanceCount() {
		return bindingServiceProperties.getInstanceCount();
	}

	public void setInstanceCount(int instanceCount) {
		bindingServiceProperties.setInstanceCount(instanceCount);
	}

	public String[] getDynamicDestinations() {
		return bindingServiceProperties.getDynamicDestinations();
	}

	public void setDynamicDestinations(String[] dynamicDestinations) {
		bindingServiceProperties.setDynamicDestinations(dynamicDestinations);
	}

	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		bindingServiceProperties.setApplicationContext(applicationContext);
	}

	public void setConversionService(ConversionService conversionService) {
		bindingServiceProperties.setConversionService(conversionService);
	}

	public void afterPropertiesSet() throws Exception {
		bindingServiceProperties.afterPropertiesSet();
	}

	public String getBinder(String channelName) {
		return bindingServiceProperties.getBinder(channelName);
	}

	public Map<String, Object> asMapProperties() {
		return bindingServiceProperties.asMapProperties();
	}

	public ConsumerProperties getConsumerProperties(String inputChannelName) {
		return bindingServiceProperties.getConsumerProperties(inputChannelName);
	}

	public ProducerProperties getProducerProperties(String outputChannelName) {
		return bindingServiceProperties.getProducerProperties(outputChannelName);
	}

	public BindingProperties getBindingProperties(String channelName) {
		return bindingServiceProperties.getBindingProperties(channelName);
	}

	public String getGroup(String channelName) {
		return bindingServiceProperties.getGroup(channelName);
	}

	public String getBindingDestination(String channelName) {
		return bindingServiceProperties.getBindingDestination(channelName);
	}
}
