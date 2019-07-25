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

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.catalog.FunctionTypeUtils;
import org.springframework.cloud.function.context.catalog.BeanFactoryAwareFunctionRegistry.FunctionInvocationWrapper;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.MessageChannelReactiveUtils;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.handler.ServiceActivatingHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
	public BeanPostProcessor functionChannelBindingPostProcessor(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			StreamFunctionProperties functionProperties, BindableProxyFactory bindableProxyFactory) {
		return new FunctionChannelBindingPostProcessor(functionCatalog, functionInspector, functionProperties, bindableProxyFactory);
	}

	private static class FunctionChannelBindingPostProcessor implements BeanPostProcessor, ApplicationContextAware {

		private final FunctionCatalog functionCatalog;

		private final FunctionInspector functionInspector;

		private final StreamFunctionProperties functionProperties;

		private final BindableProxyFactory bindableProxyFactory;

		private GenericApplicationContext context;

		FunctionChannelBindingPostProcessor(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
				StreamFunctionProperties functionProperties, BindableProxyFactory bindableProxyFactory) {
			this.functionCatalog = functionCatalog;
			this.functionInspector = functionInspector;
			this.functionProperties = functionProperties;
			this.bindableProxyFactory = bindableProxyFactory;
		}

		public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
			if (bean instanceof SubscribableChannel && functionCatalog.lookup(functionProperties.getDefinition()) != null
					&& ("input".equals(beanName) || "output".equals(beanName))) {
				this.doPostProcess(beanName, (SubscribableChannel) bean);
			}

			return bean;
		}

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.context = (GenericApplicationContext) applicationContext;
		}

		private void doPostProcess(String channelName, SubscribableChannel messageChannel) {

			//TODO there is something about moving channel interceptors in AMCB (not sure if it is still required)
			if (functionProperties.isComposeTo() && messageChannel instanceof SubscribableChannel && "input".equals(channelName)) {
				System.out.println("Composing at the tail");
			}
			else if (functionProperties.isComposeFrom() && "output".equals(channelName)) {
				System.out.println("Composing at the head");
				FunctionInvocationWrapper function = functionCatalog.lookup(functionProperties.getDefinition(), "application/json");
				ServiceActivatingHandler handler = new ServiceActivatingHandler(new FunctionWrapper(function));
				handler.setBeanFactory(context);
				handler.afterPropertiesSet();

				DirectWithAttributesChannel newOutputChannel = new DirectWithAttributesChannel();
				newOutputChannel.setAttribute("type", "output");
				newOutputChannel.setComponentName("output.extended");
				this.context.registerBean("output.extended", MessageChannel.class, () -> newOutputChannel);
				this.bindableProxyFactory.replaceOutputChannel(channelName, "output.extended", newOutputChannel);

				handler.setOutputChannelName("output.extended");
				SubscribableChannel subscribeChannel = (SubscribableChannel) messageChannel;
				subscribeChannel.subscribe(handler);

			}
			else {
				FunctionInvocationWrapper function = functionCatalog.lookup(functionProperties.getDefinition(), "application/json");
				if (function.getTarget() instanceof Supplier) {
					System.out.println("Configuring supplier");
					throw new UnsupportedOperationException("Standalone supplier are not currently supported");
				}
				else if (function.getTarget() instanceof Consumer) {

				}
				else {
					if ("input".equals(channelName)) {
						this.postProcessForStandAloneFunction(function, messageChannel);
					}
				}
			}
		}

		private void postProcessForStandAloneFunction(FunctionInvocationWrapper function, MessageChannel inputChannel) {
			Type functionType = FunctionTypeUtils.getFunctionType(function, this.functionInspector);
			if (FunctionTypeUtils.isReactive(FunctionTypeUtils.getInputType(functionType, 0))) {
				MessageChannel outputChannel = context.getBean("output", MessageChannel.class);
				SubscribableChannel subscribeChannel = (SubscribableChannel) inputChannel;
				Publisher<?> publisher = this.enhancePublisher(MessageChannelReactiveUtils.toPublisher(subscribeChannel));
				this.subscribeToInput(function, publisher, outputChannel::send);
			}
			else {
				ServiceActivatingHandler handler = new ServiceActivatingHandler(new FunctionWrapper(function));
				handler.setBeanFactory(context);
				handler.afterPropertiesSet();
				handler.setOutputChannelName("output");
				SubscribableChannel subscribeChannel = (SubscribableChannel) inputChannel;
				subscribeChannel.subscribe(handler);
			}
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private Publisher enhancePublisher(Publisher publisher) {
			Flux flux = Flux.from(publisher)
					.concatMap(message -> {
						return Flux.just(message)
								.doOnError(e -> e.printStackTrace())
								.retryBackoff(3, //this.consumerProperties.getMaxAttempts(),
										Duration.ofMillis(1000),
												//this.consumerProperties.getBackOffInitialInterval()),
										Duration.ofMillis(1000))//this.consumerProperties.getBackOffMaxInterval()));
								.onErrorResume(e -> {
									e.printStackTrace();
									//onError(e, originalMessageRef.get());
									return Mono.empty();
								});

					});
			return flux;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private <I, O> void subscribeToInput(Function function,
				Publisher<?> publisher, Consumer<Message<O>> outputProcessor) {

			Function<Flux<Message<I>>, Flux<Message<O>>> functionInvoker = function;
			Flux<?> inputPublisher = Flux.from(publisher);
			subscribeToOutput(outputProcessor,
					functionInvoker.apply((Flux<Message<I>>) inputPublisher)).subscribe();
		}

		private <O> Mono<Void> subscribeToOutput(Consumer<Message<O>> outputProcessor,
				Publisher<Message<O>> outputPublisher) {

			Flux<Message<O>> output = outputProcessor == null ? Flux.from(outputPublisher)
					: Flux.from(outputPublisher).doOnNext(outputProcessor);
			return output.then();
		}
	}
	/**
	 *
	 * Ensure that SI does not attempt any conversion and sends a raw Message
	 *
	 */
	@SuppressWarnings("rawtypes")
	private static class FunctionWrapper implements Function<Message<byte[]>, Message<byte[]>> {
		private final Function function;

		FunctionWrapper(Function function) {
			this.function = function;
		}
		@SuppressWarnings("unchecked")
		@Override
		public Message<byte[]> apply(Message<byte[]> t) {
			Message<byte[]> resultMessage =  (Message<byte[]>) function.apply(t);
			return resultMessage;
		}
	}
}
