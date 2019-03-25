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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.config.BinderFactoryConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @since 2.1
 */
@Configuration
@EnableConfigurationProperties(StreamFunctionProperties.class)
@Import(BinderFactoryConfiguration.class)
@AutoConfigureBefore(BindingServiceConfiguration.class)
public class FunctionConfiguration {

	@Bean
	public IntegrationFlowFunctionSupport functionSupport(
			FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory messageConverterFactory,
			StreamFunctionProperties functionProperties,
			BindingServiceProperties bindingServiceProperties) {
		return new IntegrationFlowFunctionSupport(functionCatalog, functionInspector,
				messageConverterFactory, functionProperties, bindingServiceProperties);
	}

	/**
	 * This configuration creates an instance of the {@link IntegrationFlow} from standard
	 * Spring Cloud Stream bindings such as {@link Source}, {@link Processor} and
	 * {@link Sink} ONLY if there are no existing instances of the {@link IntegrationFlow}
	 * already available in the context. This means that it only plays a role in
	 * green-field Spring Cloud Stream apps.
	 *
	 * For logic to compose functions into the existing apps please see
	 * "FUNCTION-TO-EXISTING-APP" section of AbstractMessageChannelBinder.
	 *
	 * The @ConditionalOnMissingBean ensures it does not collide with the the instance of
	 * the IntegrationFlow that may have been already defined by the existing (extended)
	 * app.
	 * @param functionSupport support for registering beans
	 * @param source source binding
	 * @param processor processor binding
	 * @param sink sink binding
	 * @return integration flow for Stream
	 */
	@ConditionalOnMissingBean
	@Bean
	public IntegrationFlow integrationFlowCreator(
			IntegrationFlowFunctionSupport functionSupport,
			@Nullable Source source, @Nullable Processor processor, @Nullable Sink sink) {
		if (functionSupport.containsFunction(Consumer.class)
				&& consumerBindingPresent(processor, sink)) {
			return functionSupport
					.integrationFlowForFunction(getInputChannel(processor, sink), getOutputChannel(processor, source))
					.get();
		}
		else if (functionSupport.containsFunction(Function.class)
				&& consumerBindingPresent(processor, sink)) {
			return functionSupport
					.integrationFlowForFunction(getInputChannel(processor, sink), getOutputChannel(processor, source))
					.get();
		}
		else if (functionSupport.containsFunction(Supplier.class)) {
			return functionSupport.integrationFlowFromNamedSupplier()
					.channel(getOutputChannel(processor, source)).get();
		}
		return null;
	}

	private boolean consumerBindingPresent(Processor processor, Sink sink) {
		return processor != null || sink != null;
	}

	private SubscribableChannel getInputChannel(Processor processor, Sink sink) {
		return processor != null ? processor.input() : sink.input();
	}

	private MessageChannel getOutputChannel(Processor processor, Source source) {
		return processor != null ? processor.output()
				: (source != null ? source.output() : new NullChannel());
	}

}
