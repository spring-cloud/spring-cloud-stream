/*
 * Copyright 2018-2024 the original author or authors.
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

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import io.micrometer.context.ContextSnapshotFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;
import reactor.util.function.Tuples;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.cloud.function.cloudevent.CloudEventMessageUtils;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.PollableBean;
import org.springframework.cloud.function.context.catalog.FunctionTypeUtils;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.function.context.config.FunctionContextUtils;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties.PollerProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.DefaultPartitioningInterceptor;
import org.springframework.cloud.stream.binding.NewDestinationBindingCallback;
import org.springframework.cloud.stream.binding.SupportedBindableFeatures;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.cloud.stream.utils.BuildInformationProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.type.MethodMetadata;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.util.IntegrationReactiveUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 * @author Chris Bono
 * @author Byungjun You
 * @author Ivan Shapoval
 * @author Patrik Péter Süli
 * @author Artem Bilan
 * @author Omer Celik
 * @since 2.1
 */
@Lazy(false)
@AutoConfiguration
@EnableConfigurationProperties(StreamFunctionConfigurationProperties.class)
@Import({ BinderFactoryAutoConfiguration.class })
@AutoConfigureBefore(BindingServiceConfiguration.class)
@AutoConfigureAfter(ContextFunctionCatalogAutoConfiguration.class)
@ConditionalOnBean(FunctionRegistry.class)
public class FunctionConfiguration {

	private static final boolean isContextPropagationPresent = ClassUtils.isPresent(
		"io.micrometer.context.ContextSnapshot", FunctionConfiguration.class.getClassLoader());


	@SuppressWarnings("rawtypes")
	@Bean
	public StreamBridge streamBridgeUtils(FunctionCatalog functionCatalog,
			BindingServiceProperties bindingServiceProperties, ConfigurableApplicationContext applicationContext,
			@Nullable NewDestinationBindingCallback callback) {
		return new StreamBridge(functionCatalog, bindingServiceProperties, applicationContext, callback);
	}

	@Bean
	public InitializingBean functionBindingRegistrar(Environment environment, FunctionCatalog functionCatalog,
			StreamFunctionProperties streamFunctionProperties) {
		return new FunctionBindingRegistrar(functionCatalog, streamFunctionProperties);
	}

	@Bean
	public InitializingBean functionInitializer(FunctionCatalog functionCatalog,
												StreamFunctionProperties functionProperties,
												BindingServiceProperties serviceProperties, ConfigurableApplicationContext applicationContext,
												StreamBridge streamBridge) {
		return new FunctionToDestinationBinder(functionCatalog, functionProperties,
				serviceProperties, streamBridge);
	}

	/*
	 * Binding initializer responsible only for Suppliers
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Bean
	InitializingBean supplierInitializer(FunctionCatalog functionCatalog, StreamFunctionProperties functionProperties,
			GenericApplicationContext context, BindingServiceProperties serviceProperties,
			@Nullable List<BindableFunctionProxyFactory> proxyFactories, StreamBridge streamBridge,
			TaskScheduler taskScheduler) {

		if (CollectionUtils.isEmpty(proxyFactories)) {
			return null;
		}

		return () -> {
			for (BindableFunctionProxyFactory proxyFactory : proxyFactories) {
				FunctionInvocationWrapper functionWrapper = functionCatalog.lookup(proxyFactory.getFunctionDefinition());
				if (functionWrapper != null && functionWrapper.isSupplier()) {
					// gather output content types
					List<String> contentTypes = new ArrayList<String>();
					if (proxyFactory.getOutputs().size() == 0) {
						return;
					}
					Assert.isTrue(proxyFactory.getOutputs().size() == 1, "Supplier with multiple outputs is not supported at the moment.");
					String outputName  = proxyFactory.getOutputs().iterator().next();

					BindingProperties bindingProperties = serviceProperties.getBindingProperties(outputName);
					ProducerProperties producerProperties = bindingProperties.getProducer();
					if (!(bindingProperties.getProducer() != null && producerProperties.isUseNativeEncoding())) {
						contentTypes.add(bindingProperties.getContentType());
					}

					// see https://github.com/spring-cloud/spring-cloud-stream/issues/2027
					String functionDefinition = proxyFactory.getFunctionDefinition();
					if (!StringUtils.hasText(functionDefinition)) {
						continue;
					}
					String[] functionNames = StringUtils.delimitedListToStringArray(functionDefinition.replaceAll(",", "|").trim(), "|");

					Function supplier = null;
					Function function = null;
					if (!ObjectUtils.isEmpty(functionNames) && functionNames.length > 1) {
						String supplierName = functionNames[0];
						String remainingFunctionDefinition = StringUtils
								.arrayToCommaDelimitedString(Arrays.copyOfRange(functionNames, 1, functionNames.length));

						supplier = functionCatalog.lookup(supplierName);
						function = functionCatalog.lookup(remainingFunctionDefinition, contentTypes.toArray(new String[0]));

						if (!((FunctionInvocationWrapper) supplier).isOutputTypePublisher() &&
								((FunctionInvocationWrapper) function).isInputTypePublisher()) {
							functionWrapper = null;
						}
						else {
							functionWrapper = functionCatalog.lookup(proxyFactory.getFunctionDefinition(), contentTypes.toArray(new String[0]));
						}
					}
					else {
						functionWrapper = functionCatalog.lookup(proxyFactory.getFunctionDefinition(), contentTypes.toArray(new String[0]));
					}

					if (!functionProperties.isComposeFrom() && !functionProperties.isComposeTo()) {
						String integrationFlowName = proxyFactory.getFunctionDefinition() + "_integrationflow";

						PollableBean pollable = null;
						try {
							pollable = extractPollableAnnotation(functionProperties, context, proxyFactory);
						}
						catch (Exception e) {
							// Will fix itself once https://github.com/spring-projects/spring-framework/issues/28748 is fixed
						}

						if (functionWrapper != null) {
							FunctionInvocationWrapper postProcessor = functionWrapper;
							IntegrationFlow integrationFlow = integrationFlowFromProvidedSupplier(new PartitionAwareFunctionWrapper(functionWrapper, context, producerProperties),
									pollable, context, taskScheduler, producerProperties, outputName)
									.intercept(new ChannelInterceptor() {
										public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
											postProcessor.postProcess();
										}
									})
									.route(Message.class, message -> {
										if (message.getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
											String destinationName = (String) message.getHeaders().get("spring.cloud.stream.sendto.destination");
											return streamBridge.resolveDestination(destinationName, producerProperties, null);
										}
										return outputName;
									})
									.get();
							IntegrationFlow postProcessedFlow = (IntegrationFlow) context.getAutowireCapableBeanFactory()
									.initializeBean(integrationFlow, integrationFlowName);
							context.registerBean(integrationFlowName, IntegrationFlow.class, () -> postProcessedFlow);
						}
						else {
							IntegrationFlow integrationFlow = integrationFlowFromProvidedSupplier(new PartitionAwareFunctionWrapper(supplier, context, producerProperties),
									pollable, context, taskScheduler, producerProperties, outputName)
									.channel(c -> c.direct())
									.fluxTransform((Function<? super Flux<Message<Object>>, ? extends Publisher<Object>>) function)
									.route(Message.class, message -> {
										if (message.getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
											String destinationName = (String) message.getHeaders().get("spring.cloud.stream.sendto.destination");
											return streamBridge.resolveDestination(destinationName, producerProperties, null);
										}
										return outputName;
									})
									.get();
							IntegrationFlow postProcessedFlow = (IntegrationFlow) context.getAutowireCapableBeanFactory()
									.initializeBean(integrationFlow, integrationFlowName);
							context.registerBean(integrationFlowName, IntegrationFlow.class, () -> postProcessedFlow);
						}
					}
				}
			}
		};
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private IntegrationFlowBuilder integrationFlowFromProvidedSupplier(Supplier<?> supplier,
			PollableBean pollable, GenericApplicationContext context,
			TaskScheduler taskScheduler, ProducerProperties producerProperties, String bindingName) {

		IntegrationFlowBuilder integrationFlowBuilder;

		boolean splittable = pollable != null
				&& (boolean) AnnotationUtils.getAnnotationAttributes(pollable).get("splittable");

		FunctionInvocationWrapper function =
			(supplier instanceof PartitionAwareFunctionWrapper partitionAwareFunctionWrapper)
				? (FunctionInvocationWrapper) partitionAwareFunctionWrapper.function : (FunctionInvocationWrapper) supplier;
		boolean reactive = FunctionTypeUtils.isPublisher(function.getOutputType());

		if (pollable == null && reactive) {
			Publisher publisher = (Publisher) supplier.get();
			publisher = publisher instanceof Mono
					? ((Mono) publisher).map(this::wrapToMessageIfNecessary)
					: ((Flux) publisher).map(this::wrapToMessageIfNecessary);

			integrationFlowBuilder = IntegrationFlow.from(publisher);

			// see https://github.com/spring-cloud/spring-cloud-stream/issues/1863 for details about the following code
			taskScheduler.schedule(() -> { }, Instant.now()); // will keep AC alive
		}
		else { // implies pollable
			AtomicReference<PollerMetadata> pollerMetadata = new AtomicReference<>();
			if (producerProperties != null && producerProperties.getPoller() != null) {
				PollerProperties poller = producerProperties.getPoller();

				PollerMetadata pm = new PollerMetadata();
				PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
				map.from(poller::getMaxMessagesPerPoll).to(pm::setMaxMessagesPerPoll);
				map.from(poller).as(this::asTrigger).to(pm::setTrigger);
				pollerMetadata.set(pm);
			}

			boolean autoStartup = producerProperties == null || producerProperties.isAutoStartup();
			integrationFlowBuilder = pollerMetadata == null
					? IntegrationFlow.fromSupplier(supplier,
							spca -> spca.id(bindingName + "_spca").autoStartup(autoStartup))
					: IntegrationFlow.fromSupplier(supplier, spca -> spca.id(bindingName + "_spca")
							.poller(pollerMetadata.get()).autoStartup(autoStartup));

			// only apply the PollableBean attributes if this is a reactive function.
			if (splittable && reactive) {
				integrationFlowBuilder = integrationFlowBuilder.split();
			}
		}

		return integrationFlowBuilder;
	}

	private Trigger asTrigger(PollerProperties poller) {
		if (StringUtils.hasText(poller.getCron())) {
			return new CronTrigger(poller.getCron());
		}
		return createPeriodicTrigger(poller.getFixedDelay(), poller.getInitialDelay());
	}

	private Trigger createPeriodicTrigger(Duration period, Duration initialDelay) {
		PeriodicTrigger trigger = new PeriodicTrigger(period);
		if (initialDelay != null) {
			trigger.setInitialDelay(initialDelay);
		}
		return trigger;
	}

	private PollableBean extractPollableAnnotation(StreamFunctionProperties functionProperties, GenericApplicationContext context,
			BindableFunctionProxyFactory proxyFactory) {
		// here we need to ensure that for cases where composition is defined we only look for supplier method to find Pollable annotation.
		String supplierFunctionName = StringUtils
				.delimitedListToStringArray(proxyFactory.getFunctionDefinition().replaceAll(",", "|").trim(), "|")[0];
		BeanDefinition bd = context.getBeanDefinition(supplierFunctionName);
		if (!(bd instanceof RootBeanDefinition rootBeanDefinition)) {
			return null;
		}

		Method factoryMethod = rootBeanDefinition.getResolvedFactoryMethod();
		if (factoryMethod == null) {
			Object source = bd.getSource();
			if (source instanceof MethodMetadata methodMetadata) {
				Class<?> factory = ClassUtils.resolveClassName(methodMetadata.getDeclaringClassName(), null);
				Class<?>[] params = FunctionContextUtils.getParamTypesFromBeanDefinitionFactory(factory, (RootBeanDefinition) bd);
				factoryMethod = ReflectionUtils.findMethod(factory, methodMetadata.getMethodName(), params);
			}
		}
		Assert.notNull(factoryMethod, "Failed to introspect factory method since it was not discovered for function '"
				+ functionProperties.getDefinition() + "'");
		return factoryMethod.getReturnType().isAssignableFrom(Supplier.class)
				? AnnotationUtils.findAnnotation(factoryMethod, PollableBean.class)
						: null;
	}


	@SuppressWarnings("unchecked")
	private <T> Message<T> wrapToMessageIfNecessary(T value) {
		return value instanceof Message message
				? message
						: MessageBuilder.withPayload(value).build();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Message sanitize(Message inputMessage) {
		MessageBuilder builder = MessageBuilder
				.fromMessage(inputMessage)
				.removeHeader("spring.cloud.stream.sendto.destination")
				.removeHeader(MessageUtils.SOURCE_TYPE);
		if (builder.getHeaders().containsKey(MessageUtils.TARGET_PROTOCOL)) {
			builder = builder.setHeader(MessageUtils.SOURCE_TYPE, builder.getHeaders().get(MessageUtils.TARGET_PROTOCOL));
		}
		builder = builder.removeHeader(MessageUtils.TARGET_PROTOCOL);
		return builder.build();
	}

	private static class FunctionToDestinationBinder implements InitializingBean, ApplicationContextAware {

		protected final Log logger = LogFactory.getLog(getClass());

		private GenericApplicationContext applicationContext;

		private BindableProxyFactory[] bindableProxyFactories;

		private final FunctionCatalog functionCatalog;

		private final StreamFunctionProperties functionProperties;

		private final BindingServiceProperties serviceProperties;

		private final StreamBridge streamBridge;

		FunctionToDestinationBinder(FunctionCatalog functionCatalog, StreamFunctionProperties functionProperties,
				BindingServiceProperties serviceProperties, StreamBridge streamBridge) {
			this.functionCatalog = functionCatalog;
			this.functionProperties = functionProperties;
			this.serviceProperties = serviceProperties;
			this.streamBridge = streamBridge;
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.applicationContext = (GenericApplicationContext) applicationContext;
		}

		@Override
		public void afterPropertiesSet() throws Exception {
			Map<String, BindableProxyFactory> beansOfType = applicationContext.getBeansOfType(BindableProxyFactory.class);
			this.bindableProxyFactories = beansOfType.values().toArray(new BindableProxyFactory[0]);
			for (BindableProxyFactory bindableProxyFactory : this.bindableProxyFactories) {
				String functionDefinition = bindableProxyFactory instanceof BindableFunctionProxyFactory functionFactory
					&& functionFactory.isFunctionExist()
						? functionFactory.getFunctionDefinition()
								: null; /*this.functionProperties.getDefinition();*/

				boolean shouldNotProcess = false;
				if (!(bindableProxyFactory instanceof BindableFunctionProxyFactory)) {
					Set<String> outputBindingNames = bindableProxyFactory.getOutputs();
					shouldNotProcess = !CollectionUtils.isEmpty(outputBindingNames)
							&& outputBindingNames.iterator().next().equals("applicationMetrics");
				}
				if (StringUtils.hasText(functionDefinition) && !shouldNotProcess) {
					FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition);
					if (function != null && !function.isSupplier() && functionDefinition.equals(function.getFunctionDefinition())) {
						this.bindFunctionToDestinations(bindableProxyFactory, functionDefinition, applicationContext.getEnvironment());
					}
				}
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private void bindFunctionToDestinations(BindableProxyFactory bindableProxyFactory, String functionDefinition, ConfigurableEnvironment environment) {
			this.assertBindingIsPossible(bindableProxyFactory);

			Set<String> inputBindingNames = bindableProxyFactory.getInputs();
			Set<String> outputBindingNames = bindableProxyFactory.getOutputs();

			String[] outputContentTypes = outputBindingNames.stream()
					.map(bindingName -> this.serviceProperties.getBindings().get(bindingName).getContentType())
					.toArray(String[]::new);

			FunctionInvocationWrapper function = this.functionCatalog.lookup(functionDefinition, outputContentTypes);
			this.assertSupportedSignatures(bindableProxyFactory, function);


			if (this.functionProperties.isComposeFrom()) {
				AbstractSubscribableChannel outputChannel = this.applicationContext.getBean(outputBindingNames.iterator().next(), AbstractSubscribableChannel.class);
				logger.info("Composing at the head of output destination: " + outputChannel.getBeanName());
				String outputChannelName = outputChannel.getBeanName();
				DirectWithAttributesChannel newOutputChannel = new DirectWithAttributesChannel();
				newOutputChannel.setAttribute("type", "output");
				newOutputChannel.setComponentName("output.extended");
				this.applicationContext.registerBean("output.extended", MessageChannel.class, () -> newOutputChannel);
				bindableProxyFactory.replaceOutputChannel(outputChannelName, "output.extended", newOutputChannel);
				inputBindingNames = Collections.singleton("output");
			}

			if (isReactiveOrMultipleInputOutput(bindableProxyFactory, function.getInputType(), function.getOutputType())) {
				AtomicReference<Function<Message<?>, Message<?>>> targetProtocolEnhancer = new AtomicReference<>();
				if (!CollectionUtils.isEmpty(outputBindingNames)) {
					String outputBindingName = outputBindingNames.iterator().next(); // TODO only gets the first one
					String binderConfigurationName = this.serviceProperties.getBinder(outputBindingName);
					BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class);
					final Boolean reactive = functionProperties.getReactive().get(functionDefinition);
					final boolean reactiveFn = reactive != null && reactive;
					Class<?> bindableType = MessageChannel.class;
					if (reactiveFn) {
						bindableType = FluxMessageChannel.class;
					}
					Object binder = binderFactory.getBinder(binderConfigurationName, bindableType);
					String targetProtocol = binder.getClass().getSimpleName().startsWith("Rabbit") ? "amqp" : "kafka";
					Field headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
					headersField.setAccessible(true);
					targetProtocolEnhancer.set(message -> {
						Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
								.getField(headersField, message.getHeaders());
						headersMap.putIfAbsent(MessageUtils.TARGET_PROTOCOL, targetProtocol);
						if (CloudEventMessageUtils.isCloudEvent((message))) {
							headersMap.putIfAbsent(MessageUtils.MESSAGE_TYPE, CloudEventMessageUtils.CLOUDEVENT_VALUE);
						}
						if (BuildInformationProvider.isVersionValid()) {
							headersMap.putIfAbsent(BinderHeaders.SCST_VERSION, BuildInformationProvider.getVersion());
						}
						return message;
					});
				}

				Publisher[] inputPublishers = inputBindingNames.stream().map(inputBindingName -> {
					BindingProperties bindingProperties = this.serviceProperties.getBindings().get(inputBindingName);
					ConsumerProperties consumerProperties = bindingProperties == null ? null : bindingProperties.getConsumer();
					if (consumerProperties != null) {
						function.setSkipInputConversion(consumerProperties.isUseNativeDecoding());
						if (consumerProperties.getConcurrency() > 1) {
							this.logger.warn("When using concurrency > 1 in reactive contexts, please make sure that you are using a " +
								"reactive binder that supports concurrency settings. Otherwise, concurrency settings > 1 will be ignored when " +
								"using reactive types.");
						}
					}
					MessageChannel inputChannel = this.applicationContext.getBean(inputBindingName, MessageChannel.class);
					return IntegrationReactiveUtils.messageChannelToFlux(inputChannel).map(m -> {
						if (m != null) {
							m = sanitize(m);
						}
						return m;
					});
				})
				.map(publisher -> {
					if (targetProtocolEnhancer.get() != null) {
						return publisher.map(targetProtocolEnhancer.get());
					}
					else {
						return publisher;
					}
				})
				.toArray(Publisher[]::new);

				Function functionToInvoke = function;
				if (!CollectionUtils.isEmpty(outputBindingNames)) {
					BindingProperties bindingProperties = this.serviceProperties.getBindings().get(outputBindingNames.iterator().next());
					ProducerProperties producerProperties = bindingProperties == null ? null : bindingProperties.getProducer();
					if (producerProperties != null) {
						function.setSkipOutputConversion(producerProperties.isUseNativeEncoding());
					}
					functionToInvoke = new PartitionAwareFunctionWrapper(function, this.applicationContext, producerProperties);
					// If we have a multi-output scenario, we will do any message enrichment (aka, determining the outbound
					// partition) via the corresponding reactive Flux types. Currently, we support multiple output
					// bindings for reactive types only (Tuples).
					if (outputBindingNames.size() > 1) {
						((PartitionAwareFunctionWrapper) functionToInvoke).setMessageEnricherEnabled(false);
					}
				}
				Object resultPublishers = functionToInvoke.apply(inputPublishers.length == 1 ? inputPublishers[0] : Tuples.fromArray(inputPublishers));
				if (!(resultPublishers instanceof Iterable)) {
					resultPublishers = Collections.singletonList(resultPublishers);
				}
				Iterator<String> outputBindingIter = outputBindingNames.iterator();
				long outputCount = StreamSupport.stream(((Iterable) resultPublishers).spliterator(), false).count();

				((Iterable) resultPublishers).forEach(publisher -> {
					Flux flux = Flux.from((Publisher) publisher);
					if (!CollectionUtils.isEmpty(outputBindingNames)) {
						String outputBinding = outputBindingIter.next();
						MessageChannel outputChannel = this.applicationContext.getBean(outputBinding, MessageChannel.class);
						flux = flux.doOnNext(message -> {
							// If there are more than 1 output bindings, then ensure that we properly calculate the partitions
							// based on information from the correct output binding properties.
							if (outputCount > 1) {
								Integer partitionId = determinePartitionForOutputBinding(outputBinding, message);
								message = MessageBuilder
									.fromMessage((Message<?>) message)
									.setHeader(BinderHeaders.PARTITION_HEADER, partitionId).build();
							}
							if (message instanceof Message m && m.getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
								String destinationName = (String) m.getHeaders().get("spring.cloud.stream.sendto.destination");
								ProducerProperties producerProperties = this.serviceProperties.getBindings().get(outputBindingNames.iterator().next()).getProducer();
								MessageChannel dynamicChannel = streamBridge.resolveDestination(destinationName, producerProperties, null);
								if (logger.isInfoEnabled()) {
									logger.info("Output message is sent to '" + destinationName + "' destination");
								}
								dynamicChannel.send(m);
							}
							else {
								if (!(message instanceof Message)) {
									message = MessageBuilder.withPayload(message).build();
								}
								if (isContextPropagationPresent && outputChannel instanceof FluxMessageChannel) {
									ContextView reactorContext = StaticMessageHeaderAccessor.getReactorContext((Message) message);
									try (AutoCloseable autoCloseable = ContextSnapshotHelper.setContext(reactorContext)) {
										outputChannel.send((Message) message);
									}
									catch (Exception e) {

									}
								}
								else {
									outputChannel.send((Message) message);
								}

							}
						})
						.doOnError(e -> {
							logger.error("Failure was detected during execution of the reactive function '" +  functionDefinition + "'");
							((Throwable) e).printStackTrace();
						});
					}
					if (!function.isConsumer()) {
						flux.subscribe();
					}
				});
			}
			else {
				String outputDestinationName = this.determineOutputDestinationName(0, bindableProxyFactory, function.isConsumer());
				if (!ObjectUtils.isEmpty(inputBindingNames)) {
					String inputDestinationName = inputBindingNames.iterator().next();
					Object inputDestination = this.applicationContext.getBean(inputDestinationName);
					if (inputDestination != null && inputDestination instanceof SubscribableChannel) {
						AbstractMessageHandler handler = createFunctionHandler(function, inputDestinationName, outputDestinationName);
						((SubscribableChannel) inputDestination).subscribe(handler);
					}
				}
			}
		}

		private Integer determinePartitionForOutputBinding(String outputBinding, Object message) {
			BindingProperties bindingProperties = FunctionToDestinationBinder.this.serviceProperties.getBindings().get(outputBinding);
			ProducerProperties producerProperties = bindingProperties == null ? null : bindingProperties.getProducer();
			if (producerProperties != null && producerProperties.isPartitioned()) {
				StandardEvaluationContext evaluationContext = ExpressionUtils.createStandardEvaluationContext(this.applicationContext.getBeanFactory());
				PartitionHandler partitionHandler = new PartitionHandler(evaluationContext, producerProperties, this.applicationContext.getBeanFactory());
				if (message instanceof Message) {
					return partitionHandler.determinePartition((Message<?>) message);
				}
			}
			return null;
		}

		private AbstractMessageHandler createFunctionHandler(FunctionInvocationWrapper function,
				String inputChannelName, String outputChannelName) {
			ConsumerProperties consumerProperties = StringUtils.hasText(inputChannelName)
					? this.serviceProperties.getBindingProperties(inputChannelName).getConsumer()
							: null;
			ProducerProperties producerProperties = StringUtils.hasText(outputChannelName)
					? this.serviceProperties.getBindingProperties(outputChannelName).getProducer()
							: null;

			FunctionWrapper functionInvocationWrapper = (new FunctionWrapper(function, consumerProperties,
					producerProperties, applicationContext, this.determineTargetProtocol(outputChannelName)));

			MessagingTemplate template = new MessagingTemplate();
			template.setBeanFactory(applicationContext.getBeanFactory());

			AbstractMessageHandler handler = new AbstractMessageHandler() {
				@SuppressWarnings("unchecked")
				@Override
				public void handleMessageInternal(Message<?> message) throws MessagingException {
					Object result = functionInvocationWrapper.apply((Message<byte[]>) message);
					if (result == null) {
						logger.debug("Function execution resulted in null. No message will be sent");
						return;
					}
					if (result instanceof Iterable<?> iterableResult) {
						for (Object resultElement : iterableResult) {
							this.doSendMessage(resultElement, message);
						}
					}
					else if (ObjectUtils.isArray(result) && !(result instanceof byte[])) {
						for (int i = 0; i < ((Object[]) result).length; i++) {
							this.doSendMessage(((Object[]) result)[i], message);
						}
					}
					else {
						this.doSendMessage(result, message);
					}
				}

				private void doSendMessage(Object result, Message<?> requestMessage) {
					if (result instanceof Message<?> messageResult && messageResult.getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
						String destinationName = (String) messageResult.getHeaders().get("spring.cloud.stream.sendto.destination");
						MessageChannel outputChannel = streamBridge.resolveDestination(destinationName, producerProperties, null);
						BindingProperties bindingProperties = serviceProperties.getBindingProperties(destinationName);
						ProducerProperties sendToBindingProducerProperties = bindingProperties.getProducer();
						if (sendToBindingProducerProperties != null && sendToBindingProducerProperties.isPartitioned()) {
							((AbstractMessageChannel) outputChannel)
								.addInterceptor(new DefaultPartitioningInterceptor(bindingProperties, applicationContext.getBeanFactory()));
						}
						if (logger.isInfoEnabled()) {
							logger.info("Output message is sent to '" + destinationName + "' destination");
						}
						outputChannel.send(messageResult);
					}
					else if (StringUtils.hasText(outputChannelName)) {
						if (!(result instanceof Message)) {
							result = MessageBuilder.withPayload(result).copyHeadersIfAbsent(requestMessage.getHeaders()).build();
						}
						template.send(outputChannelName, (Message<?>) result);
					}
					else if (function.isRoutingFunction()) {
						if (!(result instanceof Message)) {
							result = MessageBuilder.withPayload(result).copyHeadersIfAbsent(requestMessage.getHeaders()).build();
						}
						streamBridge.send(function.getFunctionDefinition() + "-out-0", result);
					}
					function.postProcess();
				}

			};

			handler.setBeanFactory(this.applicationContext);
			handler.afterPropertiesSet();
			return handler;
		}

		private String determineTargetProtocol(String outputBindingName) {
			if (StringUtils.hasText(outputBindingName)) {
				String binderConfigurationName = this.serviceProperties.getBinder(outputBindingName);
				BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class);
				Object binder = binderFactory.getBinder(binderConfigurationName, MessageChannel.class);
				return binder.getClass().getSimpleName().startsWith("Rabbit") ? "amqp" : "kafka";
			}
			return null;
		}

		private boolean isReactiveOrMultipleInputOutput(BindableProxyFactory bindableProxyFactory, Type inputType, Type outputType) {
			boolean reactiveInputsOutputs = FunctionTypeUtils.isPublisher(inputType) ||
					FunctionTypeUtils.isPublisher(outputType);
			return isMultipleInputOutput(bindableProxyFactory) || reactiveInputsOutputs;
		}

		private String determineOutputDestinationName(int index, BindableProxyFactory bindableProxyFactory, boolean isConsumer) {
			List<String> outputNames = new ArrayList<>(bindableProxyFactory.getOutputs());
			if (CollectionUtils.isEmpty(outputNames)) {
				outputNames = Collections.singletonList("output");
			}
			return bindableProxyFactory instanceof BindableFunctionProxyFactory
					? ((BindableFunctionProxyFactory) bindableProxyFactory).getOutputName(index)
							: (isConsumer ? null : outputNames.get(index));
		}

		private void assertBindingIsPossible(BindableProxyFactory bindableProxyFactory) {
			if (this.isMultipleInputOutput(bindableProxyFactory)) {
				Assert.isTrue(!functionProperties.isComposeTo() && !functionProperties.isComposeFrom(),
						"Composing to/from existing Sinks and Sources are not supported for functions with multiple arguments.");
			}
		}

		private boolean isMultipleInputOutput(BindableProxyFactory bindableProxyFactory) {
			return bindableProxyFactory instanceof BindableFunctionProxyFactory
					&& ((BindableFunctionProxyFactory) bindableProxyFactory).isMultiple();
		}

		private boolean isArray(Type type) {
			return type instanceof GenericArrayType || type instanceof Class && ((Class<?>) type).isArray();
		}

		private void assertSupportedSignatures(BindableProxyFactory bindableProxyFactory, FunctionInvocationWrapper function) {
			if (this.isMultipleInputOutput(bindableProxyFactory)) {
				Assert.isTrue(!function.isConsumer(),
						"Function '" + functionProperties.getDefinition() + "' is a Consumer which is not supported "
								+ "for multi-in/out reactive streams. Only Functions are supported");
				Assert.isTrue(!function.isSupplier(),
						"Function '" + functionProperties.getDefinition() + "' is a Supplier which is not supported "
								+ "for multi-in/out reactive streams. Only Functions are supported");
				Assert.isTrue(!this.isArray(function.getInputType()) && !this.isArray(function.getOutputType()),
						"Function '" + functionProperties.getDefinition() + "' has the following signature: ["
						+ function + "]. Your input and/or outout lacks arity and therefore we "
								+ "can not determine how many input/output destinations are required in the context of "
								+ "function input/output binding.");
			}
		}

	}

	/**
	 *
	 * It's signatures ensures that within the context of s-c-stream Spring Integration does
	 * not attempt any conversion and sends a raw Message.
	 */
	@SuppressWarnings("rawtypes")
	private static class FunctionWrapper implements Function<Message<byte[]>, Object> {
		private final Function function;

		private final ConsumerProperties consumerProperties;

		private final ProducerProperties producerProperties;

		private final Field headersField;

		private final ConfigurableApplicationContext applicationContext;

		private final boolean isRoutingFunction;

		private final String targetProtocol;

		FunctionWrapper(Function function, ConsumerProperties consumerProperties,
				ProducerProperties producerProperties, ConfigurableApplicationContext applicationContext, String targetProtocol) {

			isRoutingFunction = ((FunctionInvocationWrapper) function).getTarget() instanceof RoutingFunction;
			this.applicationContext = applicationContext;
			this.function = new PartitionAwareFunctionWrapper(function, this.applicationContext, producerProperties);
			this.consumerProperties = consumerProperties;
			if (this.consumerProperties != null) {
				((FunctionInvocationWrapper) function).setSkipInputConversion(this.consumerProperties.isUseNativeDecoding());
			}
			this.producerProperties = producerProperties;
			if (this.producerProperties != null) {
				((FunctionInvocationWrapper) function).setSkipOutputConversion(this.producerProperties.isUseNativeEncoding());
			}
			this.headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
			this.headersField.setAccessible(true);
			this.targetProtocol = targetProtocol;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object apply(Message<byte[]> message) {
			message = sanitize(message);
			setHeadersIfNeeded(message);
			Object result = function.apply(message);
			if (result instanceof Publisher && this.isRoutingFunction) {
				throw new IllegalStateException("Routing to functions that return Publisher "
						+ "is not supported in the context of Spring Cloud Stream.");
			}
			if (result instanceof Message<?> resultMessage) {
				setHeadersIfNeeded(resultMessage);
			}
			return result;
		}

		private void setHeadersIfNeeded(Message message) {
			Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
				.getField(this.headersField, message.getHeaders());
			if (StringUtils.hasText(targetProtocol)) {
				headersMap.putIfAbsent(MessageUtils.TARGET_PROTOCOL, targetProtocol);
			}
			if (CloudEventMessageUtils.isCloudEvent(message)) {
				headersMap.putIfAbsent(MessageUtils.MESSAGE_TYPE, CloudEventMessageUtils.CLOUDEVENT_VALUE);
			}
			if (BuildInformationProvider.isVersionValid()) {
				headersMap.putIfAbsent(BinderHeaders.SCST_VERSION, BuildInformationProvider.getVersion());
			}
		}

	}

	/**
	 * Creates and registers instances of BindableFunctionProxyFactory for each user defined function
	 * thus triggering destination bindings between function arguments and destinations.
	 *
	 * In other words this class is responsible to do the same work as EnableBinding except that it derives the input/output names
	 * from the names of the function (e.g., function-in-0).
	 */
	private static class FunctionBindingRegistrar implements InitializingBean, ApplicationContextAware, EnvironmentAware {

		protected final Log logger = LogFactory.getLog(getClass());

		private final FunctionCatalog functionCatalog;

		private final StreamFunctionProperties streamFunctionProperties;

		private ConfigurableApplicationContext applicationContext;

		private Environment environment;

		private int inputCount;

		private int outputCount;

		FunctionBindingRegistrar(FunctionCatalog functionCatalog, StreamFunctionProperties streamFunctionProperties) {
			this.functionCatalog = functionCatalog;
			this.streamFunctionProperties = streamFunctionProperties;
		}

		@Override
		public void afterPropertiesSet() throws Exception {
			this.determineFunctionName(functionCatalog, environment);

			if (StringUtils.hasText(streamFunctionProperties.getDefinition())) {
				String[] functionDefinitions = this.filterEligibleFunctionDefinitions();
				for (String functionDefinition : functionDefinitions) {

					FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition);
					if (function != null) {
						if (function.isSupplier()) {
							this.inputCount = 0;
							this.outputCount = this.getOutputCount(function, true);
						}
						else if (function.isConsumer() || function.isRoutingFunction()) {
							this.inputCount = FunctionTypeUtils.getInputCount(function);
							this.outputCount = 0;
						}
						else {
							this.inputCount = FunctionTypeUtils.getInputCount(function);
							if (function.isWrappedBiConsumer()) {
								this.outputCount = 0;
							}
							else {
								this.outputCount = this.getOutputCount(function, false);
							}
						}

						AtomicReference<BindableFunctionProxyFactory> proxyFactory = new AtomicReference<>();
						if (function.isInputTypePublisher()) {
							final SupportedBindableFeatures supportedBindableFeatures = new SupportedBindableFeatures();
							supportedBindableFeatures.setPollable(false);
							supportedBindableFeatures.setReactive(true);

							proxyFactory.set(new BindableFunctionProxyFactory(functionDefinition,
								this.inputCount, this.outputCount, this.streamFunctionProperties, supportedBindableFeatures));
						}
						else {
							proxyFactory.set(new BindableFunctionProxyFactory(functionDefinition,
								this.inputCount, this.outputCount, this.streamFunctionProperties));
						}
						((GenericApplicationContext) this.applicationContext).registerBean(functionDefinition + "_binding",
							BindableFunctionProxyFactory.class, proxyFactory::get);
					}
					else {
						logger.warn("The function definition '" + streamFunctionProperties.getDefinition() +
								"' is not valid. The referenced function bean or one of its components does not exist");
					}
				}
			}

			this.createStandAloneBindingsIfNecessary(applicationContext.getBean(BindingServiceProperties.class));

		}

		private void createStandAloneBindingsIfNecessary(BindingServiceProperties bindingProperties) {
			String[] inputBindings = StringUtils.hasText(bindingProperties.getInputBindings())
					? bindingProperties.getInputBindings().split(";") : new String[0];

			String[] outputBindings = StringUtils.hasText(bindingProperties.getOutputBindings())
					? bindingProperties.getOutputBindings().split(";") : new String[0];

			for (String inputBindingName : inputBindings) {
				FunctionInvocationWrapper sourceFunc = functionCatalog.lookup(inputBindingName);
				if (sourceFunc != null && !sourceFunc.getFunctionDefinition().equals(inputBindingName)) {
					sourceFunc = null;
				}

				if (sourceFunc == null || //see https://github.com/spring-cloud/spring-cloud-stream/issues/2229
						sourceFunc.isSupplier() ||
						(!sourceFunc.getFunctionDefinition().equals(inputBindingName) && applicationContext.containsBean(inputBindingName))) {

					BindableFunctionProxyFactory proxyFactory = new BindableFunctionProxyFactory(inputBindingName, 1, 0, this.streamFunctionProperties, sourceFunc != null);
					((GenericApplicationContext) this.applicationContext).registerBean(inputBindingName + "_binding_in",
						BindableFunctionProxyFactory.class, () -> proxyFactory);
				}
			}

			for (String outputBindingName : outputBindings) {
				FunctionInvocationWrapper sourceFunc = functionCatalog.lookup(outputBindingName);
				if (sourceFunc != null && !sourceFunc.getFunctionDefinition().equals(outputBindingName)) {
					sourceFunc = null;
				}

				if (sourceFunc == null || //see https://github.com/spring-cloud/spring-cloud-stream/issues/2229
						sourceFunc.isConsumer() ||
						(!sourceFunc.getFunctionDefinition().equals(outputBindingName) && applicationContext.containsBean(outputBindingName))) {

					BindableFunctionProxyFactory proxyFactory = new BindableFunctionProxyFactory(outputBindingName, 0, 1, this.streamFunctionProperties, sourceFunc != null);
					((GenericApplicationContext) this.applicationContext).registerBean(outputBindingName + "_binding_out",
						BindableFunctionProxyFactory.class, () -> proxyFactory);
				}
			}
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}

		@Override
		public void setEnvironment(Environment environment) {
			this.environment = environment;
		}

		private int getOutputCount(FunctionInvocationWrapper function, boolean isSupplier) {
			int outputCount = FunctionTypeUtils.getOutputCount(function);
			if (!function.isSupplier() && function.getOutputType() instanceof ParameterizedType) {
				Type outputType = function.getOutputType();
				if (FunctionTypeUtils.isMono(outputType) && outputType instanceof ParameterizedType
						&& FunctionTypeUtils.getRawType(((ParameterizedType) outputType).getActualTypeArguments()[0]).equals(Void.class)) {
					outputCount = 0;
				}
				else if (FunctionTypeUtils.getRawType(outputType).equals(Void.class)) {
					outputCount = 0;
				}
			}
			return outputCount;
		}

		private boolean determineFunctionName(FunctionCatalog catalog, Environment environment) {
			boolean autodetect = environment.getProperty("spring.cloud.stream.function.autodetect", boolean.class, true);
			String definition = streamFunctionProperties.getDefinition();
			if (!StringUtils.hasText(definition)) {
				definition = environment.getProperty("spring.cloud.function.definition");
			}

			if (StringUtils.hasText(definition)) {
				streamFunctionProperties.setDefinition(definition);
			}
			else if (Boolean.parseBoolean(environment.getProperty("spring.cloud.stream.function.routing.enabled", "false"))
					|| environment.containsProperty("spring.cloud.function.routing-expression")) {
				streamFunctionProperties.setDefinition(RoutingFunction.FUNCTION_NAME);
			}
			else if (autodetect) {
				FunctionInvocationWrapper function = functionCatalog.lookup("");
				if (function != null) {
					streamFunctionProperties.setDefinition(function.getFunctionDefinition());
				}
			}
			return StringUtils.hasText(streamFunctionProperties.getDefinition());
		}

		/*
		 * This is to accommodate Kafka streams binder, since it does not rely on binding mechanism provided by s-c-stream core.
		 * So we basically filter out any function name who's type contains KTable or KStream.
		 */
		private String[] filterEligibleFunctionDefinitions() {
			List<String> eligibleFunctionDefinitions = new ArrayList<>();
			String[] functionDefinitions = streamFunctionProperties.getDefinition().split(";");
			for (String functionDefinition : functionDefinitions) {
				functionDefinition = functionDefinition.trim();
				String[] functionNames = StringUtils.delimitedListToStringArray(functionDefinition.replaceAll(",", "|").trim(), "|");
				boolean eligibleDefinition = true;
				for (int i = 0; i < functionNames.length && eligibleDefinition; i++) {
					String functionName = functionNames[i];
					if (this.applicationContext.containsBean(functionName)) {
						Object functionBean = this.applicationContext.getBean(functionName);
						Type functionType = FunctionTypeUtils.discoverFunctionType(functionBean, functionName, (GenericApplicationContext) this.applicationContext);
						if (functionType == null) {
							eligibleDefinition = false;
						}
						else {
							String functionTypeStringValue = functionType.toString();
							if (functionTypeStringValue.contains("KTable") || functionTypeStringValue.contains("KStream")) {
								eligibleDefinition = false;
							}
						}
					}
					else {
						logger.warn("You have defined function definition that does not exist: " + functionName);
					}
				}
				if (eligibleDefinition) {
					eligibleFunctionDefinitions.add(functionDefinition);
				}
			}
			return eligibleFunctionDefinitions.toArray(new String[0]);
		}

	}

	private static final class ContextSnapshotHelper {

		private static final ContextSnapshotFactory CONTEXT_SNAPSHOT_FACTORY = ContextSnapshotFactory.builder().build();

		static AutoCloseable setContext(ContextView context) {
			return CONTEXT_SNAPSHOT_FACTORY.setThreadLocalsFrom(context);
		}

	}

}
