/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Oleg Zhurakousky
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 * @since 2.1
 */
@Configuration
@ConditionalOnProperty("spring.cloud.stream.function.definition")
@EnableConfigurationProperties(StreamFunctionProperties.class)
public class FunctionConfiguration {

	@Autowired(required = false)
	private Source source;

	@Autowired(required = false)
	private Processor processor;

	@Autowired(required = false)
	private Sink sink;

	@Bean
	public IntegrationFlowFunctionSupport functionSupport(FunctionCatalogWrapper functionCatalog,
		FunctionInspector functionInspector, CompositeMessageConverterFactory messageConverterFactory,
		StreamFunctionProperties functionProperties, BindingServiceProperties bindingServiceProperties) {
		return new IntegrationFlowFunctionSupport(functionCatalog, functionInspector, messageConverterFactory,
			functionProperties, bindingServiceProperties);
	}

	@Bean
	public FunctionCatalogWrapper functionCatalogWrapper(FunctionCatalog catalog) {
		return new FunctionCatalogWrapper(catalog);
	}

	/**
	 * This configuration creates an instance of the {@link IntegrationFlow} from standard
	 * Spring Cloud Stream bindings such as {@link Source}, {@link Processor} and {@link Sink}
	 * ONLY if there are no existing instances of the {@link IntegrationFlow} already available
	 * in the context. This means that it only plays a role in green-field Spring Cloud Stream apps.
	 *
	 * For logic to compose functions into the existing apps please see "FUNCTION-TO-EXISTING-APP"
	 * section of AbstractMessageChannelBinder.
	 *
	 * The @ConditionalOnMissingBean ensures it does not collide with the the instance of the IntegrationFlow
	 * that may have been already defined by the existing (extended) app.
	 */
	@ConditionalOnMissingBean
	@Bean
	public IntegrationFlow integrationFlowCreator(IntegrationFlowFunctionSupport functionSupport) {
		if (functionSupport.containsFunction(Consumer.class) && consumerBindingPresent()) {
			return functionSupport.integrationFlowForFunction(getInputChannel(), getOutputChannel()).get();
		}
		else if (functionSupport.containsFunction(Function.class) && consumerBindingPresent()) {
			return functionSupport.integrationFlowForFunction(getInputChannel(), getOutputChannel()).get();
		}
		else if (functionSupport.containsFunction(Supplier.class)) {
			return functionSupport.integrationFlowFromNamedSupplier().channel(getOutputChannel()).get();
		}
		return null;
	}

	private boolean consumerBindingPresent() {
		return this.processor != null || this.sink != null;
	}

	private SubscribableChannel getInputChannel() {
		return this.processor != null ? this.processor.input() : this.sink.input();
	}

	private MessageChannel getOutputChannel() {
		return this.processor != null ? this.processor.output()
				: (this.source != null ? this.source.output() : new NullChannel());
	}
}
