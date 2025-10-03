/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.DefaultBinderTypeRegistry;
import org.springframework.cloud.stream.binding.CompositeMessageChannelConfigurer;
import org.springframework.cloud.stream.binding.FluxMessageChannelBindingTargetFactory;
import org.springframework.cloud.stream.binding.MessageChannelConfigurer;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.MessageSourceBindingTargetFactory;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.function.StreamFunctionConfigurationProperties;
import org.springframework.cloud.stream.function.StreamFunctionProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.support.MapArgumentResolver;
import org.springframework.integration.handler.support.PayloadExpressionArgumentResolver;
import org.springframework.integration.handler.support.PayloadsArgumentResolver;
import org.springframework.integration.support.NullAwarePayloadArgumentResolver;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.Validator;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author David Harrigan
 */
@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
@EnableConfigurationProperties({ BindingServiceProperties.class })
@Import(ContentTypeConfiguration.class)
public class BinderFactoryAutoConfiguration {

	private static final String SPRING_CLOUD_STREAM_INTERNAL_PREFIX = "spring.cloud.stream.internal";

	private static final String SELF_CONTAINED_APP_PROPERTY_NAME = SPRING_CLOUD_STREAM_INTERNAL_PREFIX
			+ ".selfContained";

	protected final Log logger = LogFactory.getLog(getClass());

	@Value("${" + SELF_CONTAINED_APP_PROPERTY_NAME + ":}")
	private String selfContained;

	static Collection<BinderType> parseBinderConfigurations(ClassLoader classLoader,
			Resource resource) throws IOException, ClassNotFoundException {
		Properties properties = PropertiesLoaderUtils.loadProperties(resource);
		Collection<BinderType> parsedBinderConfigurations = new ArrayList<>();
		for (Map.Entry<?, ?> entry : properties.entrySet()) {
			String binderType = (String) entry.getKey();
			String[] binderConfigurationClassNames = StringUtils
					.commaDelimitedListToStringArray((String) entry.getValue());
			Class<?>[] binderConfigurationClasses = new Class[binderConfigurationClassNames.length];
			int i = 0;
			for (String binderConfigurationClassName : binderConfigurationClassNames) {
				binderConfigurationClasses[i++] = ClassUtils
						.forName(binderConfigurationClassName, classLoader);
			}
			parsedBinderConfigurations
					.add(new BinderType(binderType, binderConfigurationClasses));
		}
		return parsedBinderConfigurations;
	}



	@Bean(IntegrationContextUtils.MESSAGE_HANDLER_FACTORY_BEAN_NAME)
	public static MessageHandlerMethodFactory messageHandlerMethodFactory(
			@Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME) CompositeMessageConverter compositeMessageConverter,
			@Nullable Validator validator, ConfigurableListableBeanFactory clbf) {

		DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();
		messageHandlerMethodFactory.setMessageConverter(compositeMessageConverter);

		/*
		 * We essentially do the same thing as the
		 * DefaultMessageHandlerMethodFactory.initArgumentResolvers(..). We can't do it as
		 * custom resolvers for two reasons. 1. We would have two duplicate (compatible)
		 * resolvers, so they would need to be ordered properly to ensure these new
		 * resolvers take precedence. 2.
		 * DefaultMessageHandlerMethodFactory.initArgumentResolvers(..) puts
		 * MessageMethodArgumentResolver before custom converters thus not allowing an
		 * override which kind of proves #1.
		 *
		 * In all, all this will be obsolete once https://jira.spring.io/browse/SPR-17503
		 * is addressed and we can fall back on core resolvers
		 */
		List<HandlerMethodArgumentResolver> resolvers = new LinkedList<>();
		resolvers.add(new SmartPayloadArgumentResolver(
				compositeMessageConverter,
				validator));
		resolvers.add(new SmartMessageMethodArgumentResolver(
				compositeMessageConverter));

		resolvers.add(new HeaderMethodArgumentResolver(clbf.getConversionService(), clbf));
		resolvers.add(new HeadersMethodArgumentResolver());

		// Copy the order from Spring Integration for compatibility with SI 5.2
		resolvers.add(new PayloadExpressionArgumentResolver());
		resolvers.add(new NullAwarePayloadArgumentResolver(compositeMessageConverter));
		PayloadExpressionArgumentResolver payloadExpressionArgumentResolver = new PayloadExpressionArgumentResolver();
		payloadExpressionArgumentResolver.setBeanFactory(clbf);
		resolvers.add(payloadExpressionArgumentResolver);
		PayloadsArgumentResolver payloadsArgumentResolver = new PayloadsArgumentResolver();
		payloadsArgumentResolver.setBeanFactory(clbf);
		resolvers.add(payloadsArgumentResolver);
		MapArgumentResolver mapArgumentResolver = new MapArgumentResolver();
		mapArgumentResolver.setBeanFactory(clbf);
		resolvers.add(mapArgumentResolver);

		messageHandlerMethodFactory.setArgumentResolvers(resolvers);
		messageHandlerMethodFactory.setValidator(validator);
		return messageHandlerMethodFactory;
	}

	@Bean
	public BinderTypeRegistry binderTypeRegistry(
			ConfigurableApplicationContext configurableApplicationContext) {
		Map<String, BinderType> binderTypes = new HashMap<>();
		ClassLoader classLoader = configurableApplicationContext.getClassLoader();
		try {
			Enumeration<URL> resources = classLoader.getResources("META-INF/spring.binders");

			if (binderTypes.isEmpty() && !Boolean.valueOf(this.selfContained)
					&& (resources == null || !resources.hasMoreElements())) {
				this.logger.debug(
						"Failed to locate 'META-INF/spring.binders' resources on the classpath."
								+ " Assuming standard boot 'META-INF/spring.factories' configuration is used");
			}
			else {
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					UrlResource resource = new UrlResource(url);
					for (BinderType binderType : parseBinderConfigurations(classLoader, resource)) {
						binderTypes.put(binderType.getDefaultName(), binderType);
					}
				}
			}
		}
		catch (IOException | ClassNotFoundException e) {
			throw new BeanCreationException("Cannot create binder factory:", e);
		}
		return new DefaultBinderTypeRegistry(binderTypes);
	}

	@Bean
	public StreamFunctionProperties streamFunctionProperties(@Nullable StreamFunctionConfigurationProperties properties) {
		StreamFunctionProperties streamFunctionProperties = new StreamFunctionProperties();
		if (properties != null) {
			streamFunctionProperties.setBindings(properties.getBindings());
		}
		return streamFunctionProperties;
	}

	@Bean
	public MessageConverterConfigurer messageConverterConfigurer(
			BindingServiceProperties bindingServiceProperties,
			@Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME) CompositeMessageConverter compositeMessageConverter,
			@Nullable StreamFunctionProperties streamFunctionProperties) {

		return new MessageConverterConfigurer(bindingServiceProperties, compositeMessageConverter, streamFunctionProperties);
	}


	@Bean
	public SubscribableChannelBindingTargetFactory channelFactory(
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		return new SubscribableChannelBindingTargetFactory(
				compositeMessageChannelConfigurer);
	}

	@Bean
	public FluxMessageChannelBindingTargetFactory fluxMessageChannelBindingTargetFactory(
		CompositeMessageChannelConfigurer compositeMessageChannelConfigurer, GenericApplicationContext context) {
		return new FluxMessageChannelBindingTargetFactory(
			compositeMessageChannelConfigurer, context);
	}

	@Bean
	public MessageSourceBindingTargetFactory messageSourceFactory(
			@Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME) CompositeMessageConverter compositeMessageConverter,
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		return new MessageSourceBindingTargetFactory(compositeMessageConverter,
				compositeMessageChannelConfigurer);
	}

	@Bean
	public CompositeMessageChannelConfigurer compositeMessageChannelConfigurer(
			MessageConverterConfigurer messageConverterConfigurer) {
		List<MessageChannelConfigurer> configurerList = new ArrayList<>();
		configurerList.add(messageConverterConfigurer);
		return new CompositeMessageChannelConfigurer(configurerList);
	}

}
