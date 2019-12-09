/*
 * Copyright 2018-2019 the original author or authors.
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
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.function.Tuples;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.PollableBean;
import org.springframework.cloud.function.context.catalog.BeanFactoryAwareFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.catalog.FunctionTypeUtils;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.function.context.config.FunctionContextUtils;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingBeansRegistrar;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;
import org.springframework.core.type.MethodMetadata;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.MessageChannelReactiveUtils;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.ServiceActivatingHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @since 2.1
 */
@Configuration
@EnableConfigurationProperties(StreamFunctionProperties.class)
@Import({ BindingBeansRegistrar.class, BinderFactoryAutoConfiguration.class })
@AutoConfigureBefore(BindingServiceConfiguration.class)
@AutoConfigureAfter(ContextFunctionCatalogAutoConfiguration.class)
@ConditionalOnBean(FunctionRegistry.class)
public class FunctionConfiguration {

	@Bean
	public InitializingBean functionBindingRegistrar(Environment environment, FunctionCatalog functionCatalog,
			StreamFunctionProperties streamFunctionProperties, BinderTypeRegistry binderTypeRegistry) {
		return new FunctionBindingRegistrar(binderTypeRegistry, functionCatalog, streamFunctionProperties);
	}

	@Bean
	public InitializingBean functionInitializer(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			StreamFunctionProperties functionProperties, @Nullable BindableProxyFactory[] bindableProxyFactories,
			BindingServiceProperties serviceProperties, ConfigurableApplicationContext applicationContext,
			FunctionBindingRegistrar bindingHolder, BinderAwareChannelResolver dynamicDestinationResolver) {

		boolean shouldCreateInitializer = bindableProxyFactories != null
				&& (applicationContext.containsBean("output")
				|| ObjectUtils.isEmpty(applicationContext.getBeanNamesForAnnotation(EnableBinding.class)));

		return shouldCreateInitializer
				? new FunctionChannelBindingInitializer(functionCatalog, functionProperties, bindableProxyFactories,
						serviceProperties, dynamicDestinationResolver)
						: null;

	}

	/*
	 * Binding initializer responsible only for Suppliers
	 */
	@Bean
	InitializingBean supplierInitializer(FunctionCatalog functionCatalog, StreamFunctionProperties functionProperties,
			GenericApplicationContext context, BindingServiceProperties serviceProperties,
			@Nullable BindableFunctionProxyFactory[] proxyFactories, BinderAwareChannelResolver dynamicDestinationResolver,
			TaskScheduler taskScheduler) {

		if (!ObjectUtils.isEmpty(context.getBeanNamesForAnnotation(EnableBinding.class)) || proxyFactories == null) {
			return null;
		}

		return new InitializingBean() {

			@Override
			public void afterPropertiesSet() throws Exception {
				for (BindableFunctionProxyFactory proxyFactory : proxyFactories) {
					FunctionInvocationWrapper functionWrapper = functionCatalog.lookup(proxyFactory.getFunctionDefinition());
					if (functionWrapper != null && functionWrapper.isSupplier()) {
						// gather output content types
						List<String> contentTypes = new ArrayList<String>();
						Assert.isTrue(proxyFactory.getOutputs().size() == 1, "Supplier with multiple outputs is not supported at the moment.");
						for (String outputName : proxyFactory.getOutputs()) {
							BindingProperties bindingProperties = serviceProperties.getBindingProperties(outputName);
							String contentType = bindingProperties.getProducer() != null && bindingProperties.getProducer().isUseNativeEncoding()
									? null : bindingProperties.getContentType();
							contentTypes.add(contentType);
						}
						// obtain function wrapper with proper output content types
						functionWrapper = functionCatalog.lookup(proxyFactory.getFunctionDefinition(), contentTypes.toArray(new String[0]));
						Publisher<Object> beginPublishingTrigger = setupBindingTrigger(context);

						//!!!!!! TEMPORARY (see assertion about multiple outputs above)
						String outputName = proxyFactory.getOutputs().iterator().next();
						// end temporary
						if (!functionProperties.isComposeFrom() && !functionProperties.isComposeTo()) {
							String integrationFlowName = proxyFactory.getFunctionDefinition() + "_integrationflow";
							PollableBean pollable = extractPollableAnnotation(functionProperties, context, proxyFactory);

							IntegrationFlow integrationFlow = integrationFlowFromProvidedSupplier(functionWrapper, beginPublishingTrigger,
									pollable, context, taskScheduler)
									.route(Message.class, message -> {
										if (message.getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
											String destinationName = (String) message.getHeaders().get("spring.cloud.stream.sendto.destination");
											return dynamicDestinationResolver.resolveDestination(destinationName);
										}
										return outputName;
									}).get();
							IntegrationFlow postProcessedFlow = (IntegrationFlow) context.getAutowireCapableBeanFactory()
									.applyBeanPostProcessorsBeforeInitialization(integrationFlow, integrationFlowName);
							context.registerBean(integrationFlowName, IntegrationFlow.class, () -> postProcessedFlow);
						}
					}
				}
			}
		};
	}

	/*
	 * Creates a publishing trigger to ensure Supplier does not begin publishing until binding is created
	 */
	private Publisher<Object> setupBindingTrigger(GenericApplicationContext context) {
		AtomicReference<MonoSink<Object>> triggerRef = new AtomicReference<>();
		Publisher<Object> beginPublishingTrigger = Mono.create(emmiter -> {
			triggerRef.set(emmiter);
		});
		context.addApplicationListener(event -> {
			if (event instanceof BindingCreatedEvent) {
				if (triggerRef.get() != null) {
					triggerRef.get().success();
				}
			}
		});
		return beginPublishingTrigger;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private IntegrationFlowBuilder integrationFlowFromProvidedSupplier(Supplier<?> supplier,
			Publisher<Object> beginPublishingTrigger, PollableBean pollable, GenericApplicationContext context,
			TaskScheduler taskScheduler) {

		IntegrationFlowBuilder integrationFlowBuilder;
		Type functionType = ((FunctionInvocationWrapper) supplier).getFunctionType();

		boolean splittable = pollable != null
				&& (boolean) AnnotationUtils.getAnnotationAttributes(pollable).get("splittable");

		if (pollable == null && FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, 0))) {
			Publisher publisher = (Publisher) supplier.get();
			publisher = publisher instanceof Mono
					? ((Mono) publisher).delaySubscription(beginPublishingTrigger).map(this::wrapToMessageIfNecessary)
					: ((Flux) publisher).delaySubscription(beginPublishingTrigger).map(this::wrapToMessageIfNecessary);

			integrationFlowBuilder = IntegrationFlows.from(publisher);

			// see https://github.com/spring-cloud/spring-cloud-stream/issues/1863 for details about the following code
			taskScheduler.schedule(() -> { }, Instant.now()); // will keep AC alive
		}
		else { // implies pollable
			integrationFlowBuilder = IntegrationFlows.from(supplier);
			if (splittable) {
				integrationFlowBuilder = integrationFlowBuilder.split();
			}
		}

		return integrationFlowBuilder;
	}

	private PollableBean extractPollableAnnotation(StreamFunctionProperties functionProperties, GenericApplicationContext context,
			BindableFunctionProxyFactory proxyFactory) {
		// here we need to ensure that for cases where composition is defined we only look for supplier method to find Pollable annotation.
		String supplierFunctionName = StringUtils
				.delimitedListToStringArray(proxyFactory.getFunctionDefinition().replaceAll(",", "|").trim(), "|")[0];
		BeanDefinition bd = context.getBeanDefinition(supplierFunctionName);
		if (!(bd instanceof RootBeanDefinition)) {
			return null;
		}

		Method factoryMethod = ((RootBeanDefinition) bd).getResolvedFactoryMethod();
		if (factoryMethod == null) {
			Object source = bd.getSource();
			if (source instanceof MethodMetadata) {
				Class<?> factory = ClassUtils.resolveClassName(((MethodMetadata) source).getDeclaringClassName(), null);
				Class<?>[] params = FunctionContextUtils.getParamTypesFromBeanDefinitionFactory(factory, (RootBeanDefinition) bd);
				factoryMethod = ReflectionUtils.findMethod(factory, ((MethodMetadata) source).getMethodName(), params);
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
		return value instanceof Message
				? (Message<T>) value
						: MessageBuilder.withPayload(value).build();
	}

	/*
	 * Binding initializer responsible only for Functions and Consumers.
	 */
	private static class FunctionChannelBindingInitializer implements InitializingBean, ApplicationContextAware {

		private static Log logger = LogFactory.getLog(FunctionChannelBindingInitializer.class);

		private final FunctionCatalog functionCatalog;

		private final StreamFunctionProperties functionProperties;

		private final BindableProxyFactory[] bindableProxyFactories;

		private final BindingServiceProperties serviceProperties;

		private final BinderAwareChannelResolver dynamicDestinationResolver;

		private GenericApplicationContext context;


		FunctionChannelBindingInitializer(FunctionCatalog functionCatalog, StreamFunctionProperties functionProperties,
				BindableProxyFactory[] bindableProxyFactories, BindingServiceProperties serviceProperties,
				BinderAwareChannelResolver dynamicDestinationResolver) {
			this.functionCatalog = functionCatalog;
			this.functionProperties = functionProperties;
			this.bindableProxyFactories = bindableProxyFactories;
			this.serviceProperties = serviceProperties;
			this.dynamicDestinationResolver = dynamicDestinationResolver;
		}

		@Override
		public void afterPropertiesSet() throws Exception {
			Stream.of(this.bindableProxyFactories).forEach(bindableProxyFactory -> {
				String functionDefinition = getFunctionDefinition(bindableProxyFactory);
				FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition);
				if (function != null && !function.isSupplier()) {
					if (isMultipleInputOutput(bindableProxyFactory)) {
						this.bindMultipleArgumentsFunction(bindableProxyFactory, functionDefinition);
					}
					else {
						SubscribableChannel messageChannel = this.determineChannelToSubscribeTo(bindableProxyFactory);
						if (messageChannel != null) {
							this.bindOrComposeSimpleFunctions(messageChannel, bindableProxyFactory, functionDefinition);
						}
					}
				}
			});
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.context = (GenericApplicationContext) applicationContext;
		}

		private String getFunctionDefinition(BindableProxyFactory bindableProxyFactory) {
			return bindableProxyFactory instanceof BindableFunctionProxyFactory
					? ((BindableFunctionProxyFactory) bindableProxyFactory).getFunctionDefinition()
							: this.functionProperties.getDefinition();
		}

		private SubscribableChannel determineChannelToSubscribeTo(BindableProxyFactory bindableProxyFactory) {
			SubscribableChannel messageChannel = null;
			if (bindableProxyFactory instanceof BindableFunctionProxyFactory) {
				String channelName = ((BindableFunctionProxyFactory) bindableProxyFactory).getInputName(0);
				messageChannel = this.context.getBean(channelName, SubscribableChannel.class);
			}
			else {
				if (this.context.containsBean("input")) {
					logger.info("@EnableBinding way of defining channels is not supported by functions, so 'input' "
							+ "channel will not be bound to any existing function beans. You may safely ignore this "
							+ "message if that was not your intention otherwise, please remove @EnableBinding annotation.");
				}
				if (this.context.containsBean("output")) { // need this to compose to existing sources
					messageChannel = this.context.getBean("output", SubscribableChannel.class);
				}
			}
			return messageChannel;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void bindMultipleArgumentsFunction(BindableProxyFactory bindableProxyFactory, String functionDefinition) {
			Assert.isTrue(!functionProperties.isComposeTo() && !functionProperties.isComposeFrom(),
					"Composing to/from existing Sinks and Sources are not supported for functions with multiple arguments.");

			BindableFunctionProxyFactory functionProxyFactory = (BindableFunctionProxyFactory) bindableProxyFactory;
			Set<String> inputBindingNames = functionProxyFactory.getInputs();
			Set<String> outputBindingNames = functionProxyFactory.getOutputs();

			String[] outputContentTypes = outputBindingNames.stream()
					.map(bindingName -> this.serviceProperties.getBindings().get(bindingName).getContentType())
					.toArray(String[]::new);

			FunctionInvocationWrapper function = this.functionCatalog.lookup(functionDefinition, outputContentTypes);

			if (isMultipleInputOutput(bindableProxyFactory)) {
				this.assertSupportedSignatures(function.getFunctionType());
			}

			Publisher[] inputPublishers = inputBindingNames.stream().map(inputBindingName -> {
				SubscribableChannel inputChannel = this.context.getBean(inputBindingName, SubscribableChannel.class);
				return MessageChannelReactiveUtils.toPublisher(inputChannel);
			}).toArray(Publisher[]::new);


			Object resultPublishers = function.apply(inputPublishers.length == 1 ? inputPublishers[0] : Tuples.fromArray(inputPublishers));
			if (resultPublishers instanceof Iterable) {
				Iterator<String> outputBindingIter = outputBindingNames.iterator();
				((Iterable) resultPublishers).forEach(publisher -> {
					MessageChannel outputChannel = this.context.getBean(outputBindingIter.next(), MessageChannel.class);
					Flux.from((Publisher) publisher).doOnNext(message -> outputChannel.send((Message) message)).subscribe();
				});
			}
			else {
				outputBindingNames.stream().forEach(outputBindingName -> {
					MessageChannel outputChannel = this.context.getBean(outputBindingName, MessageChannel.class);
					Flux.from((Publisher) resultPublishers).doOnNext(message -> outputChannel.send((Message) message)).subscribe();
				});
			}
		}

		/*
		 * Will either bind function to destination or compose it to the existing flow
		 */
		private void bindOrComposeSimpleFunctions(SubscribableChannel messageChannel,
				BindableProxyFactory bindableProxyFactory, String functionDefinition) {
			BindingProperties properties = this.serviceProperties.getBindingProperties(((AbstractMessageChannel) messageChannel).getBeanName());
			FunctionInvocationWrapper function = this.functionCatalog.lookup(functionDefinition, properties.getContentType());

			if (this.functionProperties.isComposeFrom()) {
				logger.info("Composing at the head of 'output' channel");
				this.composeSimpleFunctionToExistingFlow(function, messageChannel, bindableProxyFactory);
			}
			else {
				this.bindSimpleFunctions(function, messageChannel, bindableProxyFactory);
			}
		}

		private void composeSimpleFunctionToExistingFlow(FunctionInvocationWrapper function, SubscribableChannel outputChannel,
				BindableProxyFactory bindableProxyFactory) {
			String outputChannelName = ((AbstractMessageChannel) outputChannel).getBeanName();
			ServiceActivatingHandler handler = createFunctionHandler(function, null, outputChannelName);

			DirectWithAttributesChannel newOutputChannel = new DirectWithAttributesChannel();
			newOutputChannel.setAttribute("type", "output");
			newOutputChannel.setComponentName("output.extended");
			this.context.registerBean("output.extended", MessageChannel.class, () -> newOutputChannel);
			bindableProxyFactory.replaceOutputChannel(outputChannelName, "output.extended", newOutputChannel);

			handler.setOutputChannelName("output.extended");
			outputChannel.subscribe(handler);
		}

		private void bindSimpleFunctions(FunctionInvocationWrapper function, SubscribableChannel inputChannel, BindableProxyFactory bindableProxyFactory) {
			Type functionType = function.getFunctionType();
			String outputChannelName = bindableProxyFactory instanceof BindableFunctionProxyFactory
					? ((BindableFunctionProxyFactory) bindableProxyFactory).getOutputName(0)
							: (FunctionTypeUtils.isConsumer(functionType) ? null : "output");

			if (FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, 0)) && StringUtils.hasText(outputChannelName)) {
				MessageChannel outputChannel = context.getBean(outputChannelName, MessageChannel.class);


				Publisher<Message<Object>> publisher = MessageChannelReactiveUtils.toPublisher(inputChannel);
				String bindingName = ((DirectWithAttributesChannel) inputChannel).getBeanName();
				this.subscribeToInput(function, bindingName, publisher, message -> outputChannel.send((Message<?>) message));
			}
			else {
				String inputChannelName = ((AbstractMessageChannel) inputChannel).getBeanName();
				ServiceActivatingHandler handler = createFunctionHandler(function, inputChannelName, outputChannelName);
				if (!FunctionTypeUtils.isConsumer(functionType)) {
					handler.setOutputChannelName(outputChannelName);
				}
				inputChannel.subscribe(handler);
			}
		}

		private ServiceActivatingHandler createFunctionHandler(FunctionInvocationWrapper function,
				String inputChannelName, String outputChannelName) {
			ConsumerProperties consumerProperties = StringUtils.hasText(inputChannelName)
					? this.serviceProperties.getBindingProperties(inputChannelName).getConsumer()
							: null;
			ProducerProperties producerProperties = StringUtils.hasText(outputChannelName)
					? this.serviceProperties.getBindingProperties(outputChannelName).getProducer()
							: null;
			ServiceActivatingHandler handler = new ServiceActivatingHandler(new FunctionWrapper(function, consumerProperties, producerProperties)) {
				protected void sendOutputs(Object result, Message<?> requestMessage) {
					if (result instanceof Message && ((Message<?>) result).getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
						String destinationName = (String) ((Message<?>) result).getHeaders().get("spring.cloud.stream.sendto.destination");
						MessageChannel outputChannel = dynamicDestinationResolver.resolveDestination(destinationName);
						if (logger.isInfoEnabled()) {
							logger.info("Output message is sent to '" + destinationName + "' destination");
						}
						outputChannel.send(((Message<?>) result));
					}
					else {
						super.sendOutputs(result, requestMessage);
					}
				}
			};
			handler.setBeanFactory(this.context);
			handler.afterPropertiesSet();
			return handler;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void subscribeToInput(Function function, String bindingName,  Publisher publisher, Consumer<Message> outputProcessor) {

			Flux<Message> inputPublisher = Flux.from(publisher);

			AtomicReference<Message<Object>> originalMessageRef = new AtomicReference<>();
			AtomicReference<ConsumerProperties> consumerPropertiesRef = new AtomicReference<>();
			AtomicReference<MessageChannel> bindingErrorChannelRef = new AtomicReference<>(context.getBean("nullChannel", MessageChannel.class));

			Flux<Message> result = inputPublisher
					.switchOnFirst((x, message) -> {
						consumerPropertiesRef.set(this.serviceProperties.getBindings().get(bindingName).getConsumer());
						String destination = serviceProperties.getBindings().get(bindingName).getDestination();
						String group = serviceProperties.getBindings().get(bindingName).getGroup();
						String bindingErrorChannelName = destination + "." + group + ".errors";
						if (context.containsBean(bindingErrorChannelName)) {
							bindingErrorChannelRef.set(context.getBean(bindingErrorChannelName, MessageChannel.class));
						}
						return message;
					})
					.concatMap(message -> {
						return Flux.just(message).doOnNext(originalMessageRef::set)
								.transform((Function<Flux<Message>, Flux<Message>>) function)
								.retryBackoff(consumerPropertiesRef.get().getMaxAttempts(),
										Duration.ofMillis(consumerPropertiesRef.get().getBackOffInitialInterval()),
										Duration.ofMillis(consumerPropertiesRef.get().getBackOffMaxInterval()))
								.onErrorResume(e -> {
									bindingErrorChannelRef.get()
										.send(new ErrorMessage((Throwable) e, originalMessageRef.get().getHeaders(), originalMessageRef.get()));
									return Mono.empty();
								});
					});

			subscribeToOutput(outputProcessor, result).subscribe();
		}

		@SuppressWarnings("rawtypes")
		private Mono<Void> subscribeToOutput(Consumer<Message> outputProcessor, Flux<Message> resultPublisher) {
			Flux<Message> output = outputProcessor == null
					? resultPublisher
							: resultPublisher.doOnNext(outputProcessor);
			return output.then();
		}

		private void assertSupportedSignatures(Type functionType) {
			Assert.isTrue(!FunctionTypeUtils.isConsumer(functionType),
					"Function '" + functionProperties.getDefinition() + "' is a Consumer which is not supported "
							+ "for multi-in/out reactive streams. Only Functions are supported");
			Assert.isTrue(!FunctionTypeUtils.isSupplier(functionType),
					"Function '" + functionProperties.getDefinition() + "' is a Supplier which is not supported "
							+ "for multi-in/out reactive streams. Only Functions are supported");
			Assert.isTrue(!FunctionTypeUtils.isInputArray(functionType) && !FunctionTypeUtils.isOutputArray(functionType),
					"Function '" + functionProperties.getDefinition() + "' has the following signature: ["
					+ functionType + "]. Your input and/or outout lacks arity and therefore we "
							+ "can not determine how many input/output destinations are required in the context of "
							+ "function input/output binding.");

			int inputCount = FunctionTypeUtils.getInputCount(functionType);
			for (int i = 0; i < inputCount; i++) {
				Assert.isTrue(FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, i)),
						"Function '" + functionProperties.getDefinition() + "' has the following signature: ["
								+ functionType + "]. Non-reactive functions with multiple "
								+ "inputs/outputs are not supported in the context of Spring Cloud Stream.");
			}
			int outputCount = FunctionTypeUtils.getOutputCount(functionType);
			for (int i = 0; i < outputCount; i++) {
				Assert.isTrue(FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, i)),
						"Function '" + functionProperties.getDefinition() + "' has the following signature: ["
								+ functionType + "]. Non-reactive functions with multiple "
								+ "inputs/outputs are not supported in the context of Spring Cloud Stream.");
			}

		}

		private boolean isMultipleInputOutput(BindableProxyFactory bindableProxyFactory) {
			return bindableProxyFactory instanceof BindableFunctionProxyFactory
					&& ((BindableFunctionProxyFactory) bindableProxyFactory).isMultiple();
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

		@SuppressWarnings("unused")
		private final ProducerProperties producerProperties;

		private final Field headersField;

		FunctionWrapper(Function function, ConsumerProperties consumerProperties, ProducerProperties producerProperties) {
			this.function = function;
			this.consumerProperties = consumerProperties;
			this.producerProperties = producerProperties;
			this.headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
			this.headersField.setAccessible(true);
		}
		@SuppressWarnings("unchecked")
		@Override
		public Message<byte[]> apply(Message<byte[]> message) {

			if (message != null && consumerProperties != null) {
				Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
						.getField(this.headersField, message.getHeaders());
				headersMap.put(FunctionProperties.SKIP_CONVERSION_HEADER, consumerProperties.isUseNativeDecoding());
			}

			Object result = function.apply(message);
			if (result instanceof Publisher) {
				throw new IllegalStateException("Routing to functions that return Publisher is not supported "
						+ "in the context of Spring Cloud Stream.");
			}
			return (Message<byte[]>) result;
		}
	}

	/**
	 * Creates and registers instances of BindableFunctionProxyFactory for each user defined function
	 * thus triggering destination bindings between function arguments and destinations.
	 */
	private static class FunctionBindingRegistrar implements InitializingBean, ApplicationContextAware, EnvironmentAware {

		private final BinderTypeRegistry binderTypeRegistry;

		private final FunctionCatalog functionCatalog;

		private final StreamFunctionProperties streamFunctionProperties;

		private ConfigurableApplicationContext applicationContext;

		private Environment environment;

		private int inputCount;

		private int outputCount;

		FunctionBindingRegistrar(BinderTypeRegistry binderTypeRegistry, FunctionCatalog functionCatalog, StreamFunctionProperties streamFunctionProperties) {
			this.binderTypeRegistry = binderTypeRegistry;
			this.functionCatalog = functionCatalog;
			this.streamFunctionProperties = streamFunctionProperties;
		}

		@Override
		public void afterPropertiesSet() throws Exception {
			if (ObjectUtils.isEmpty(applicationContext.getBeanNamesForAnnotation(EnableBinding.class))
					&& this.determineFunctionName(functionCatalog, environment)) {
				BeanDefinitionRegistry registry = (BeanDefinitionRegistry) applicationContext.getBeanFactory();
				String[] functionDefinitions = streamFunctionProperties.getDefinition().split(";");
				for (String functionDefinition : functionDefinitions) {
					RootBeanDefinition functionBindableProxyDefinition = new RootBeanDefinition(BindableFunctionProxyFactory.class);
					FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition);
					if (function != null) {
						Type functionType = function.getFunctionType();
						if (function.isSupplier()) {
							this.inputCount = 0;
							this.outputCount = FunctionTypeUtils.getOutputCount(functionType);
						}
						else if (function.isConsumer()) {
							this.inputCount = FunctionTypeUtils.getInputCount(functionType);
							this.outputCount = 0;
						}
						else {
							this.inputCount = FunctionTypeUtils.getInputCount(functionType);
							this.outputCount = FunctionTypeUtils.getOutputCount(functionType);
						}

						functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(functionDefinition);
						functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.inputCount);
						functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.outputCount);
						functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(streamFunctionProperties);
						registry.registerBeanDefinition(functionDefinition + "_binding", functionBindableProxyDefinition);
					}
				}
			}
		}

		private boolean determineFunctionName(FunctionCatalog catalog, Environment environment) {
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
			else {
				streamFunctionProperties.setDefinition(((FunctionInspector) functionCatalog).getName(functionCatalog.lookup("")));
			}
			return StringUtils.hasText(streamFunctionProperties.getDefinition());
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}

		@Override
		public void setEnvironment(Environment environment) {
			this.environment = environment;
		}
	}
}
