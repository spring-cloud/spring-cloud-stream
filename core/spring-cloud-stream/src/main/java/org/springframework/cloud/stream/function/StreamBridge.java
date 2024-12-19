/*
 * Copyright 2020-2024 the original author or authors.
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

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import io.micrometer.context.ContextExecutorService;
import io.micrometer.context.ContextSnapshotFactory;
import io.micrometer.observation.ObservationRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistration;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.PassThruFunction;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.function.core.FunctionInvocationHelper;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderWrapper;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.DefaultPartitioningInterceptor;
import org.springframework.cloud.stream.binding.NewDestinationBindingCallback;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.config.GlobalChannelInterceptorProcessor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import static org.springframework.cloud.stream.utils.CacheKeyCreatorUtils.createChannelCacheKey;


/**
 * A class which allows user to send data to an output binding.
 * While in a common scenario of a typical spring-cloud-stream application user rarely
 * has to manually send data, there are times when the sources of data are outside
 * spring-cloud-stream context, and therefore we need to bridge such foreign sources
 * with spring-cloud-stream.
 * <br><br>
 * This utility class allows user to do just that - <i>bridge non-spring-cloud-stream applications
 * with spring-cloud-stream</i> by providing a mechanism (bridge) to send data to an output binding while
 * maintaining the  same invocation contract (i.e., type conversion, partitioning etc.) as if it was
 * done through a declared function.
 *
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author Byungjun You
 * @author Michał Rowicki
 * @author Omer Celik
 * @since 3.0.3
 *
 */
@SuppressWarnings("rawtypes")
public final class StreamBridge implements StreamOperations, SmartInitializingSingleton, DisposableBean, ApplicationListener<ApplicationEvent> {

	private static final String STREAM_BRIDGE_FUNC_NAME = "streamBridge";

	private final Log logger = LogFactory.getLog(getClass());

	private final Map<String, MessageChannel> channelCache;

	private final FunctionCatalog functionCatalog;

	private final NewDestinationBindingCallback destinationBindingCallback;

	private final BindingServiceProperties bindingServiceProperties;

	private final ConfigurableApplicationContext applicationContext;

	private boolean initialized;

	private boolean async;

	private final BindingService bindingService;

	private final Map<Integer, FunctionInvocationWrapper> streamBridgeFunctionCache;

	private final FunctionInvocationHelper<?> functionInvocationHelper;

	private ExecutorService executorService;

	private static final boolean isContextPropagationPresent = ClassUtils.isPresent(
			"io.micrometer.context.ContextSnapshotFactory", StreamBridge.class.getClassLoader());

	private static final ReentrantLock lock = new ReentrantLock();

	private ObservationRegistry observationRegistry = ObservationRegistry.NOOP;
	/**
	 *
	 * @param functionCatalog instance of {@link FunctionCatalog}
	 * @param bindingServiceProperties instance of {@link BindingServiceProperties}
	 * @param applicationContext instance of {@link ConfigurableApplicationContext}
	 */
	@SuppressWarnings("serial")
	StreamBridge(FunctionCatalog functionCatalog, BindingServiceProperties bindingServiceProperties,
		ConfigurableApplicationContext applicationContext, @Nullable NewDestinationBindingCallback destinationBindingCallback, ObjectProvider<ObservationRegistry> observationRegistries) {
		this.executorService = Executors.newCachedThreadPool();
		Assert.notNull(functionCatalog, "'functionCatalog' must not be null");
		Assert.notNull(applicationContext, "'applicationContext' must not be null");
		Assert.notNull(bindingServiceProperties, "'bindingServiceProperties' must not be null");
		this.bindingService = applicationContext.getBean(BindingService.class);
		this.functionCatalog = functionCatalog;
		this.applicationContext = applicationContext;
		this.bindingServiceProperties = bindingServiceProperties;
		this.destinationBindingCallback = destinationBindingCallback;
		this.channelCache = new LinkedHashMap<String, MessageChannel>() {
			@Override
			protected boolean removeEldestEntry(Map.Entry<String, MessageChannel> eldest) {
				boolean remove = size() > bindingServiceProperties.getDynamicDestinationCacheSize();
				if (remove) {
					if (logger.isDebugEnabled()) {
						logger.debug("Removing message channel from cache " + eldest.getKey());
					}
					bindingService.unbindProducers(eldest.getKey());
				}
				return remove;
			}
		};
		this.functionInvocationHelper = applicationContext.getBean(FunctionInvocationHelper.class);
		this.streamBridgeFunctionCache = new HashMap<>();
		observationRegistries.ifAvailable(registry -> this.observationRegistry = registry);
	}

	@Override
	public boolean send(String bindingName, Object data) {
		var contentType = determineContentType(bindingName, this.bindingServiceProperties);
		return this.send(bindingName, data, contentType);
	}

	@Override
	public boolean send(String bindingName, Object data, MimeType outputContentType) {
		return this.send(bindingName, null, data, outputContentType);
	}
	@Override
	public boolean send(String bindingName, @Nullable String binderName, Object data) {
		var contentType = determineContentType(bindingName, this.bindingServiceProperties);
		return this.send(bindingName, binderName, data, contentType);
	}

	private static MimeType determineContentType(String bindingName, BindingServiceProperties bindingServiceProperties) {
		var bindingProperties = bindingServiceProperties.getBindingProperties(bindingName);
		return StringUtils.hasText(bindingProperties.getContentType()) ?
			MimeType.valueOf(bindingProperties.getContentType()) : MimeTypeUtils.APPLICATION_JSON;
	}

	@Override
	@SuppressWarnings({ "unchecked"})
	public boolean send(String bindingName, @Nullable String binderName, Object data, MimeType outputContentType) {
		if (!this.initialized) {
			this.afterSingletonsInstantiated();
		}
		ProducerProperties producerProperties = this.bindingServiceProperties.getProducerProperties(bindingName);
		MessageChannel messageChannel = this.resolveDestination(bindingName, producerProperties, binderName);

		Function functionToInvoke;
		lock.lock();
		try {
			functionToInvoke = this.getStreamBridgeFunction(outputContentType.toString(), producerProperties);
		}
		finally {
			lock.unlock();
		}


		if (producerProperties != null && producerProperties.isPartitioned()) {
			functionToInvoke = new PartitionAwareFunctionWrapper(functionToInvoke, this.applicationContext, producerProperties);
		}

		String targetType = this.resolveBinderTargetType(bindingName, binderName, MessageChannel.class,
			this.applicationContext.getBean(BinderFactory.class));

		Message<?> messageToSend = data instanceof Message messageData
				? MessageBuilder.fromMessage(messageData).setHeaderIfAbsent(MessageUtils.TARGET_PROTOCOL, targetType).build()
						: new GenericMessage<>(data, Collections.singletonMap(MessageUtils.TARGET_PROTOCOL, targetType));

		Message<?> resultMessage;
		lock.lock();
		try {
			resultMessage = (Message<byte[]>) functionToInvoke.apply(messageToSend);
		}
		finally {
			lock.unlock();
		}

		if (resultMessage == null
			&& ((Message) messageToSend).getPayload().getClass().getName().equals("org.springframework.kafka.support.KafkaNull")) {
			resultMessage = messageToSend;
		}

		resultMessage = (Message<?>) this.functionInvocationHelper.postProcessResult(resultMessage, null);

		return messageChannel.send(resultMessage);
	}

	private int hashProducerProperties(ProducerProperties producerProperties, String outputContentType) {
		int hash = outputContentType.hashCode()
				+ Boolean.hashCode(producerProperties.isUseNativeEncoding())
				+ Boolean.hashCode(producerProperties.isPartitioned())
				+ producerProperties.getPartitionCount();

		return hash;
	}

	private FunctionInvocationWrapper getStreamBridgeFunction(String outputContentType, ProducerProperties producerProperties) {
		int streamBridgeFunctionKey = this.hashProducerProperties(producerProperties, outputContentType);
		if (this.streamBridgeFunctionCache.containsKey(streamBridgeFunctionKey)) {
			return this.streamBridgeFunctionCache.get(streamBridgeFunctionKey);
		}
		else {
			FunctionInvocationWrapper functionToInvoke = this.functionCatalog.lookup(STREAM_BRIDGE_FUNC_NAME, outputContentType.toString());
			this.streamBridgeFunctionCache.put(streamBridgeFunctionKey, functionToInvoke);
			functionToInvoke.setSkipOutputConversion(producerProperties.isUseNativeEncoding());
			return functionToInvoke;
		}
	}

	@Override
	public void afterSingletonsInstantiated() {
		if (this.initialized) {
			return;
		}

		FunctionRegistration<Function<Object, Object>> fr = new FunctionRegistration<>(new PassThruFunction(), STREAM_BRIDGE_FUNC_NAME);
		fr.getProperties().put("singleton", "false");

		Type functionType = ResolvableType.forClassWithGenerics(Function.class, Object.class, Object.class).getType();
		((FunctionRegistry) this.functionCatalog).register(fr.type(functionType));
		this.initialized = true;
	}

	@SuppressWarnings({ "unchecked"})
	MessageChannel resolveDestination(String destinationName, ProducerProperties producerProperties, String binderName) {
		lock.lock();
		try {
			MessageChannel messageChannel = this.channelCache.get(createChannelCacheKey(binderName, destinationName, bindingServiceProperties));
			if (messageChannel == null) {
				if (this.applicationContext.containsBean(destinationName)) {
					messageChannel = this.applicationContext.getBean(destinationName, MessageChannel.class);
					String[] consumerBindingNames = this.bindingService.getConsumerBindingNames();
					if (messageChannel instanceof AbstractMessageChannel) {
						addPartitioningInterceptorIfNeedBe(producerProperties, destinationName, (AbstractMessageChannel) messageChannel);
					}
					if (ObjectUtils.containsElement(consumerBindingNames, destinationName)) { //GH-2563
						logger.warn("You seem to be sending data to the input binding.  It is not "
								+ "recommended, since you are bypassing the binder and this the messaging system exposed by the binder.");
					}
				}
				else {
					messageChannel = this.isAsync() ? new ExecutorChannel(this.executorService) : new DirectWithAttributesChannel();
					((AbstractSubscribableChannel) messageChannel).setApplicationContext(applicationContext);
					((AbstractSubscribableChannel) messageChannel).setComponentName(destinationName);

					BinderWrapper binderWrapper = bindingService.createBinderWrapper(binderName, destinationName, messageChannel.getClass());
					((AbstractSubscribableChannel) messageChannel).registerObservationRegistry(observationRegistry);
					if (this.destinationBindingCallback != null) {
						Object extendedProducerProperties = this.bindingService
								.getExtendedProducerProperties(binderWrapper.binder(), destinationName);
						this.destinationBindingCallback.configure(destinationName, messageChannel,
								producerProperties, extendedProducerProperties);
					}

					addPartitioningInterceptorIfNeedBe(producerProperties, destinationName, (AbstractMessageChannel) messageChannel);
					addGlobalChannelInterceptorProcessor((AbstractMessageChannel) messageChannel, destinationName);

					this.bindingService.bindProducer(messageChannel, true, binderWrapper);
					this.channelCache.put(binderWrapper.cacheKey(), messageChannel);
				}
			}

			return messageChannel;
		}
		finally {
			lock.unlock();
		}
	}

	private void addPartitioningInterceptorIfNeedBe(ProducerProperties producerProperties, String destinationName, AbstractMessageChannel messageChannel) {
		// since we already perform the partition finding algorithm once via StreamBridge#send we don't need to
		// do the following, unless the conversion is handled natively on the middleware.
		if (producerProperties != null && producerProperties.isPartitioned() && producerProperties.isUseNativeEncoding()) {
			BindingProperties bindingProperties = this.bindingServiceProperties.getBindingProperties(destinationName);
			messageChannel
				.addInterceptor(new DefaultPartitioningInterceptor(bindingProperties, this.applicationContext.getBeanFactory()));
		}
	}

	private String resolveBinderTargetType(String channelName, String binderName, Class<?> bindableType, BinderFactory binderFactory) {
		String binderConfigurationName = binderName != null ? binderName : this.bindingServiceProperties
				.getBinder(channelName);
		Binder<?, ?, ?> binder = binderFactory.getBinder(binderConfigurationName, bindableType);
		String targetProtocol = binder.getClass().getSimpleName().startsWith("Rabbit") ? "amqp" : "kafka";
		return targetProtocol;
	}

	private void addGlobalChannelInterceptorProcessor(AbstractMessageChannel messageChannel, String destinationName) {
		final GlobalChannelInterceptorProcessor globalChannelInterceptorProcessor =
			this.applicationContext.getBean(GlobalChannelInterceptorProcessor.class);
		globalChannelInterceptorProcessor.postProcessAfterInitialization(messageChannel, destinationName);
	}

	@Override
	public void destroy() throws Exception {
		this.executorService.shutdown();
		if (!this.executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
			logger.warn("Failed to terminate executor. Terminating current tasks.");
			this.executorService.shutdownNow();
		}

		this.executorService = null;
		this.async = false;
		channelCache.keySet().forEach(bindingService::unbindProducers);
		channelCache.clear();
	}

	public boolean isAsync() {
		return async;
	}

	public void setAsync(boolean async) {
		if (isContextPropagationPresent) {
			this.executorService = ContextPropagationHelper.wrap(this.executorService);
		}
		this.async = async;
	}

	// see https://github.com/spring-cloud/spring-cloud-stream/issues/3054
	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		// we need to do it by String to avoid cloud-bus and context dependencies
		if (event.getClass().getName().equals("org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent")) {
			this.channelCache.clear();
		}
	}

	private static final class ContextPropagationHelper {
		static ExecutorService wrap(ExecutorService executorService) {
			return ContextExecutorService.wrap(executorService, () -> ContextSnapshotFactory.builder().build().captureAll());
		}
	}
}
