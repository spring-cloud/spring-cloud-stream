/*
 * Copyright 2020-2020 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * A class which allows user to send data to an output binding.
 * While in a common scenario of a typical spring-cloud-stream application user rarely
 * has to manually send data, there are times when the sources of data are outside of
 * spring-cloud-stream context and therefore we need to bridge such foreign sources
 * with spring-cloud-stream.
 * <br><br>
 * This utility class allows user to do just that - <i>bridge non-spring-cloud-stream applications
 * with spring-cloud-stream</i> by providing a mechanism (bridge) to send data to an output binding while
 * maintaining the  same invocation contract (i.e., type conversion, partitioning etc) as if it was
 * done through a declared function.
 *
 * @author Oleg Zhurakousky
 * @since 3.0.3
 *
 */
public final class StreamBridge implements SmartInitializingSingleton {

	protected final Log logger = LogFactory.getLog(getClass());

	private final Map<String, SubscribableChannel> channelCache;

	private final FunctionCatalog functionCatalog;

	private final FunctionRegistry functionRegistry;

	private BindingServiceProperties bindingServiceProperties;

	private ConfigurableApplicationContext applicationContext;

	private boolean initialized;

	@Autowired
	private BindingService bindingService;

	/**
	 *
	 * @param functionCatalog instance of {@link FunctionCatalog}
	 * @param functionRegistry instance of {@link FunctionRegistry}
	 * @param bindingServiceProperties instance of {@link BindingServiceProperties}
	 * @param applicationContext instance of {@link ConfigurableApplicationContext}
	 */
	@SuppressWarnings("serial")
	StreamBridge(FunctionCatalog functionCatalog, FunctionRegistry functionRegistry,
			BindingServiceProperties bindingServiceProperties, ConfigurableApplicationContext applicationContext) {
		this.functionCatalog = functionCatalog;
		this.functionRegistry = functionRegistry;
		this.applicationContext = applicationContext;
		this.bindingServiceProperties = bindingServiceProperties;
		this.channelCache = new LinkedHashMap<String, SubscribableChannel>() {
			@Override
			protected boolean removeEldestEntry(Map.Entry<String, SubscribableChannel> eldest) {
				boolean remove = size() > bindingServiceProperties.getDynamicDestinationCacheSize();
				if (remove && logger.isDebugEnabled()) {
					logger.debug("Removing message channel from cache " + eldest.getKey());
				}
				return remove;
			}
		};
	}

	/**
	 * Sends 'data' to an output binding specified by 'bindingName' argument while
	 * using default content type to deal with output type conversion (if necessary).
	 * @param bindingName the name of the output binding
	 * @param data the data to send
	 * @return true if data was sent successfully, otherwise false or throws an exception.
	 */
	public boolean send(String bindingName, Object data) {
		return this.send(bindingName, data, MimeTypeUtils.APPLICATION_JSON);
	}

	/**
	 * Sends 'data' to an output binding specified by 'bindingName' argument while
	 * using the content type specified by the 'outputContentType' argument to deal
	 * with output type conversion (if necessary).
	 * For typical cases `bindingName` is configured using 'spring.cloud.stream.source' property.
	 * However, this operation also supports sending to truly dynamic destinations. This means if the name
	 * provided via 'bindingName' does not have a corresponding binding such name will be
	 * treated as dynamic destination.
	 *
	 * @param bindingName the name of the output binding
	 * @param data the data to send
	 * @param outputContentType content type to be used to deal with output type conversion
	 * @return true if data was sent successfully, otherwise false or throws an exception.
	 */
	@SuppressWarnings({ "unchecked", "unused" })
	public boolean send(String bindingName, Object data, MimeType outputContentType) {
		SubscribableChannel messageChannel = this.channelCache.get(bindingName);
		ProducerProperties producerProperties = this.bindingServiceProperties.getProducerProperties(bindingName);
		if (messageChannel == null) {
			producerProperties.setRequiredGroups(bindingName);
			FunctionRegistration<Function<Object, Object>> fr = new FunctionRegistration<>(v -> v, bindingName);
			this.functionRegistry.register(fr.type(FunctionType.from(Object.class).to(Object.class).message()));
			messageChannel = new DirectWithAttributesChannel();
			Binding<SubscribableChannel> binding = this.bindingService.bindProducer(messageChannel, bindingName, false);
			this.channelCache.put(bindingName, messageChannel);
		}

		Function<Object, Object> functionToInvoke = this.functionCatalog.lookup(bindingName, outputContentType.toString());
		if (producerProperties != null && producerProperties.isPartitioned()) {
			functionToInvoke = new PartitionAwareFunctionWrapper((FunctionInvocationWrapper) functionToInvoke, this.applicationContext, producerProperties);
		}
		// this function is a pass through and is only required to force output conversion if necessary on SCF side.
		Message<byte[]> resultMessage = (Message<byte[]>) functionToInvoke.apply(data);
		return messageChannel.send(resultMessage);
	}

	@Override
	public void afterSingletonsInstantiated() {
		if (this.initialized) {
			return;
		}
		Map<String, DirectWithAttributesChannel> channels = applicationContext.getBeansOfType(DirectWithAttributesChannel.class);
		for (Entry<String, DirectWithAttributesChannel> channelEntry : channels.entrySet()) {
			if (channelEntry.getValue().getAttribute("type").equals("output")) {
				this.channelCache.put(channelEntry.getKey(), channelEntry.getValue());
				// we're registering a dummy pass-through function to ensure that it goes through the
				// same process (type conversion, etc) as other function invocation.
				FunctionRegistration<Function<Object, Object>> fr = new FunctionRegistration<>(v -> v, channelEntry.getKey());
				this.functionRegistry.register(fr.type(FunctionType.from(Object.class).to(Object.class).message()));
				this.initialized = true;
			}
		}
	}
}
