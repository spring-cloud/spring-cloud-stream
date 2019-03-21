/*
 * Copyright 2015-2018 the original author or authors.
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
import java.lang.reflect.Field;
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
import org.springframework.cloud.stream.binding.MessageChannelConfigurer;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.binding.MessageSourceBindingTargetFactory;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.support.HandlerMethodArgumentResolversHolder;
import org.springframework.lang.Nullable;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.Validator;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 * @author David Harrigan
 * @deprecated since it really represents 'auto-configuration' it will be
 * renamed/restructured in the next release.
 */
@Configuration
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
@EnableConfigurationProperties({ BindingServiceProperties.class })
@Import({ ContentTypeConfiguration.class })
@Deprecated
public class BinderFactoryConfiguration {

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
			CompositeMessageConverterFactory compositeMessageConverterFactory,
			@Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVERS_BEAN_NAME) HandlerMethodArgumentResolversHolder ahmar,
			@Nullable Validator validator, ConfigurableListableBeanFactory clbf) {

		DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();
		messageHandlerMethodFactory.setMessageConverter(
				compositeMessageConverterFactory.getMessageConverterForAllRegistered());

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
				compositeMessageConverterFactory.getMessageConverterForAllRegistered(),
				validator));
		resolvers.add(new SmartMessageMethodArgumentResolver(
				compositeMessageConverterFactory.getMessageConverterForAllRegistered()));
		resolvers.add(new HeaderMethodArgumentResolver(null, clbf));
		resolvers.add(new HeadersMethodArgumentResolver());
		resolvers.addAll(ahmar.getResolvers());

		// modify HandlerMethodArgumentResolversHolder
		Field field = ReflectionUtils
				.findField(HandlerMethodArgumentResolversHolder.class, "resolvers");
		field.setAccessible(true);
		((List<?>) ReflectionUtils.getField(field, ahmar)).clear();
		resolvers.forEach(ahmar::addResolver);
		// --

		messageHandlerMethodFactory.setArgumentResolvers(resolvers);
		messageHandlerMethodFactory.setValidator(validator);
		return messageHandlerMethodFactory;
	}

	@Bean
	public BinderTypeRegistry binderTypeRegistry(
			ConfigurableApplicationContext configurableApplicationContext) {
		Map<String, BinderType> binderTypes = new HashMap<>();
		ClassLoader classLoader = configurableApplicationContext.getClassLoader();
		// the above can never be null since it will default to
		// ClassUtils.getDefaultClassLoader(..)
		try {
			Enumeration<URL> resources = classLoader
					.getResources("META-INF/spring.binders");
			if (!Boolean.valueOf(this.selfContained)
					&& (resources == null || !resources.hasMoreElements())) {
				this.logger.debug(
						"Failed to locate 'META-INF/spring.binders' resources on the classpath."
								+ " Assuming standard boot 'META-INF/spring.factories' configuration is used");
			}
			else {
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					UrlResource resource = new UrlResource(url);
					for (BinderType binderType : parseBinderConfigurations(classLoader,
							resource)) {
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
	public MessageConverterConfigurer messageConverterConfigurer(
			BindingServiceProperties bindingServiceProperties,
			CompositeMessageConverterFactory compositeMessageConverterFactory) {
		return new MessageConverterConfigurer(bindingServiceProperties,
				compositeMessageConverterFactory);
	}

	@Bean
	public SubscribableChannelBindingTargetFactory channelFactory(
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		return new SubscribableChannelBindingTargetFactory(
				compositeMessageChannelConfigurer);
	}

	@Bean
	public MessageSourceBindingTargetFactory messageSourceFactory(
			CompositeMessageConverterFactory compositeMessageConverterFactory,
			CompositeMessageChannelConfigurer compositeMessageChannelConfigurer) {
		return new MessageSourceBindingTargetFactory(
				compositeMessageConverterFactory.getMessageConverterForAllRegistered(),
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
