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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Iterator;
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
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.PollableSupplier;
import org.springframework.cloud.function.context.catalog.BeanFactoryAwareFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.catalog.FunctionTypeUtils;
import org.springframework.cloud.function.context.config.FunctionContextUtils;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.stream.annotation.BindingProvider;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.BindingCreatedEvent;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
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
import org.springframework.integration.channel.MessageChannelReactiveUtils;
import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.ServiceActivatingHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
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
@Import(BinderFactoryAutoConfiguration.class)
@AutoConfigureBefore(BindingServiceConfiguration.class)
public class FunctionConfiguration {

	@Bean
	public InitializingBean functionBindingRegistrar(Environment environment, FunctionCatalog functionCatalog,
			StreamFunctionProperties streamFunctionProperties, BinderTypeRegistry binderTypeRegistry) {
		return new FunctionBindingRegistrar(binderTypeRegistry, functionCatalog, streamFunctionProperties);
	}

	@Bean
	public InitializingBean functionInitializer(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			StreamFunctionProperties functionProperties, @Nullable BindableProxyFactory[] bindableProxyFactories,
			BindingServiceProperties serviceProperties, ConfigurableApplicationContext applicationContext, FunctionBindingRegistrar bindingHolder) {

		if (bindableProxyFactories == null) {
			return null;
		}

		return new FunctionChannelBindingInitializer(functionCatalog, functionInspector, functionProperties, bindableProxyFactories, serviceProperties);
	}

	/*
	 * Binding initializer responsible only for Suppliers only
	 */
	@Bean
	IntegrationFlow supplierInitializer(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			StreamFunctionProperties functionProperties, GenericApplicationContext context) {
		if (!ObjectUtils.isEmpty(context.getBeanNamesForAnnotation(EnableBinding.class))) {
			return null;
		}

		IntegrationFlow integrationFlow = null;
		String[] functionDefinitions = StringUtils.hasText(functionProperties.getDefinition())
				? functionProperties.getDefinition().split(";")
						: new String[] {};

		for (String functionDefinition : functionDefinitions) {
			FunctionInvocationWrapper functionWrapper = functionCatalog.lookup(functionDefinition);
			if (functionWrapper != null && functionWrapper.isSupplier()) {
				Publisher<Object> beginPublishingTrigger = this.setupBindingTrigger(context);

				RootBeanDefinition bd = (RootBeanDefinition) context.getBeanDefinition(functionProperties.getParsedDefinition()[0]);
				Method factoryMethod = bd.getResolvedFactoryMethod();
				if (factoryMethod == null) {
					Object source = bd.getSource();
					if (source instanceof MethodMetadata) {
						Class<?> factory = ClassUtils.resolveClassName(((MethodMetadata) source).getDeclaringClassName(), null);
						Class<?>[] params = FunctionContextUtils.getParamTypesFromBeanDefinitionFactory(factory, bd);
						factoryMethod = ReflectionUtils.findMethod(factory, ((MethodMetadata) source).getMethodName(), params);
					}
				}
				Assert.notNull(factoryMethod, "Failed to introspect factory method since it was not discovered for function '"
								+ functionProperties.getDefinition() + "'");
				PollableSupplier pollable = factoryMethod.getReturnType().isAssignableFrom(Supplier.class)
						? AnnotationUtils.findAnnotation(factoryMethod, PollableSupplier.class)
								: null;

				if (!functionProperties.isComposeFrom() && !functionProperties.isComposeTo()) {
					integrationFlow = this.integrationFlowFromProvidedSupplier(functionWrapper, functionInspector, beginPublishingTrigger, pollable)
							.channel("output").get();
				}
			}
		}
		return integrationFlow;
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
			FunctionInspector inspector, Publisher<Object> beginPublishingTrigger, PollableSupplier pollable) {

		IntegrationFlowBuilder integrationFlowBuilder;
		Type functionType = FunctionTypeUtils.getFunctionType(supplier, inspector);


		boolean splittable = pollable != null && (boolean) AnnotationUtils.getAnnotationAttributes(pollable).get("splittable");

		if (pollable == null && FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, 0))) {
			Publisher publisher = (Publisher) supplier.get();
			publisher = publisher instanceof Mono
					? ((Mono) publisher).delaySubscription(beginPublishingTrigger).map(this::wrapToMessageIfNecessary)
							: ((Flux) publisher).delaySubscription(beginPublishingTrigger).map(this::wrapToMessageIfNecessary);

			integrationFlowBuilder  = IntegrationFlows.from(publisher);
		}
		else { // implies pollable
			integrationFlowBuilder = IntegrationFlows.from(supplier);
			if (splittable) {
				integrationFlowBuilder = integrationFlowBuilder.split();
			}
		}

		return integrationFlowBuilder;
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

		private final FunctionInspector functionInspector;

		private final StreamFunctionProperties functionProperties;

		private final BindableProxyFactory[] bindableProxyFactories;

		private final BindingServiceProperties serviceProperties;

		private GenericApplicationContext context;


		FunctionChannelBindingInitializer(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
				StreamFunctionProperties functionProperties, BindableProxyFactory[] bindableProxyFactories, BindingServiceProperties serviceProperties) {
			this.functionCatalog = functionCatalog;
			this.functionInspector = functionInspector;
			this.functionProperties = functionProperties;
			this.bindableProxyFactories = bindableProxyFactories;
			this.serviceProperties = serviceProperties;
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
						if (messageChannel != null && function != null) {
							this.bindOrComposeSimpleFunctions(((IntegrationObjectSupport) messageChannel).getComponentName(),
									(SubscribableChannel) messageChannel, bindableProxyFactory, functionDefinition);
						}
					}
				}
			});
		}

		private String getFunctionDefinition(BindableProxyFactory bindableProxyFactory) {
			return bindableProxyFactory instanceof BindableFunctionProxyFactory
					? ((BindableFunctionProxyFactory) bindableProxyFactory).getFunctionDefinition()
							: functionProperties.getDefinition();
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.context = (GenericApplicationContext) applicationContext;
		}

		private SubscribableChannel determineChannelToSubscribeTo(BindableProxyFactory bindableProxyFactory) {
			SubscribableChannel messageChannel = null;
			if (bindableProxyFactory instanceof BindableFunctionProxyFactory) {
				String channelName = ((BindableFunctionProxyFactory) bindableProxyFactory).getInputName(0);
				messageChannel = context.getBean(channelName, SubscribableChannel.class);
			}
			else {
				// could be "input" or "output" if subscribing to existing Source
				if (context.containsBean(Sink.INPUT)) {
					messageChannel = context.getBean(Sink.INPUT, SubscribableChannel.class);
				}
				else if (context.containsBean(Source.OUTPUT)) {
					messageChannel = context.getBean(Source.OUTPUT, SubscribableChannel.class);
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

			FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition, outputContentTypes);

			if (isMultipleInputOutput(bindableProxyFactory)) {
				this.assertSupportedSignatures(function.getFunctionType());
			}

			Publisher[] inputPublishers = inputBindingNames.stream().map(inputBindingName -> {
				SubscribableChannel inputChannel = context.getBean(inputBindingName, SubscribableChannel.class);
				return this.enhancePublisher(MessageChannelReactiveUtils.toPublisher(inputChannel), inputBindingName);
			}).toArray(Publisher[]::new);


			Object resultPublishers = function.apply(inputPublishers.length == 1 ? inputPublishers[0] : Tuples.fromArray(inputPublishers));
			if (resultPublishers instanceof Iterable) {
				Iterator<String> outputBindingIter = outputBindingNames.iterator();
				((Iterable) resultPublishers).forEach(publisher -> {
					MessageChannel outputChannel = context.getBean(outputBindingIter.next(), MessageChannel.class);
					Flux.from((Publisher) publisher).doOnNext(message -> outputChannel.send((Message) message)).subscribe();
				});
			}
			else {
				outputBindingNames.stream().forEach(outputBindingName -> {
					MessageChannel outputChannel = context.getBean(outputBindingName, MessageChannel.class);
					Flux.from((Publisher) resultPublishers).doOnNext(message -> outputChannel.send((Message) message)).subscribe();
				});
			}
		}

		//
		private void bindOrComposeSimpleFunctions(String channelName, SubscribableChannel messageChannel,
				BindableProxyFactory bindableProxyFactory, String functionDefinition) {
			//TODO there is something about moving channel interceptors in AMCB (not sure if it is still required)
			String channelType = (String) ((DirectWithAttributesChannel) messageChannel).getAttribute("type");
			if (Source.OUTPUT.equals(channelType) && functionProperties.isComposeFrom()) {
				logger.info("Composing at the head of 'output' channel");
				BindingProperties properties = this.serviceProperties.getBindings().get(channelName);
				FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition, properties.getContentType());
				this.composeSimpleFunctionToExistingFlow(function, messageChannel, channelName, bindableProxyFactory);
			}
			else {
				BindingProperties properties = this.serviceProperties.getBindings().get(channelName);
				FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition, properties.getContentType());
				this.bindSimpleFunctions(function, messageChannel, bindableProxyFactory);
			}
		}

		private void composeSimpleFunctionToExistingFlow(FunctionInvocationWrapper function, SubscribableChannel messageChannel,
				String channelName, BindableProxyFactory bindableProxyFactory) {
			ServiceActivatingHandler handler = createFunctionHandler(function);

			DirectWithAttributesChannel newOutputChannel = new DirectWithAttributesChannel();
			newOutputChannel.setAttribute("type", "output");
			newOutputChannel.setComponentName("output.extended");
			this.context.registerBean("output.extended", MessageChannel.class, () -> newOutputChannel);
			bindableProxyFactory.replaceOutputChannel(channelName, "output.extended", newOutputChannel);

			handler.setOutputChannelName("output.extended");
			messageChannel.subscribe(handler);
		}

		private void bindSimpleFunctions(FunctionInvocationWrapper function, SubscribableChannel inputChannel, BindableProxyFactory bindableProxyFactory) {
			Type functionType = FunctionTypeUtils.getFunctionType(function, this.functionInspector);
			String outputChannelName = bindableProxyFactory instanceof BindableFunctionProxyFactory
					? ((BindableFunctionProxyFactory) bindableProxyFactory).getOutputName(0)
							: Source.OUTPUT;

			if (FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, 0))) {
				MessageChannel outputChannel = context.getBean(outputChannelName, MessageChannel.class);
				SubscribableChannel subscribeChannel = (SubscribableChannel) inputChannel;
				Publisher<?> publisher = this.enhancePublisher(MessageChannelReactiveUtils.toPublisher(subscribeChannel),
																((DirectWithAttributesChannel) inputChannel).getBeanName());
				this.subscribeToInput(function, publisher, message -> outputChannel.send((Message<?>) message));
			}
			else {
				ServiceActivatingHandler handler = createFunctionHandler(function);
				if (!FunctionTypeUtils.isConsumer(functionType)) {
					handler.setOutputChannelName(outputChannelName);
				}
				inputChannel.subscribe(handler);
			}
		}

		private ServiceActivatingHandler createFunctionHandler(FunctionInvocationWrapper function) {
			ServiceActivatingHandler handler = new ServiceActivatingHandler(new FunctionWrapper(function));
			handler.setBeanFactory(context);
			handler.afterPropertiesSet();
			return handler;
		}

		/*
		 * Enhance publisher to add error handling, retries etc.
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		private Publisher enhancePublisher(Publisher publisher, String bindingName) {
			Flux flux = Flux.from(publisher)
					.concatMap(message -> {
						ConsumerProperties consumerProperties = this.serviceProperties.getBindings().get(bindingName).getConsumer();
						return Flux.just(message)
								.doOnError(e -> {
									e.printStackTrace();
								})
								.retryBackoff(
										consumerProperties.getMaxAttempts(),
										Duration.ofMillis(consumerProperties.getBackOffInitialInterval()),
										Duration.ofMillis(consumerProperties.getBackOffMaxInterval())
								)
								.onErrorResume(e -> {
									e.printStackTrace();
									return Mono.empty();
								});

					});
			return flux;
		}


		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void subscribeToInput(Function function, Publisher publisher, Consumer<Message> outputProcessor) {
			Function<Flux<Message>, Flux<Message>> functionInvoker = function;
			Flux<?> inputPublisher = Flux.from(publisher);
			subscribeToOutput(outputProcessor, functionInvoker.apply((Flux<Message>) inputPublisher)).subscribe();
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

		FunctionWrapper(Function function) {
			this.function = function;
		}
		@SuppressWarnings("unchecked")
		@Override
		public Message<byte[]> apply(Message<byte[]> t) {
			Object result = function.apply(t);
			if (result instanceof Publisher) {
				throw new IllegalStateException("Routing to functions that return Publisher is not supported in the context of Spring Cloud Stream.");
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
			Class<?>[] configurationClasses = binderTypeRegistry.getAll().values().iterator().next()
					.getConfigurationClasses();
			boolean bindingProvider = Stream.of(configurationClasses)
					.filter(clazz -> AnnotationUtils.findAnnotation(clazz, BindingProvider.class) != null)
					.findFirst().isPresent();
			if (!bindingProvider
					&& ObjectUtils.isEmpty(applicationContext.getBeanNamesForAnnotation(EnableBinding.class))
					&& this.determineFunctionName(functionCatalog, environment)) {
				BeanDefinitionRegistry registry = (BeanDefinitionRegistry) applicationContext.getBeanFactory();
				String[] functionDefinitions = streamFunctionProperties.getDefinition().split(";");
				boolean nameBasedOnFunctionName = functionDefinitions.length > 1;
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
						functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(nameBasedOnFunctionName);
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
			else if (Boolean.parseBoolean(environment.getProperty("spring.cloud.stream.function.routing.enabled", "false"))) {
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
