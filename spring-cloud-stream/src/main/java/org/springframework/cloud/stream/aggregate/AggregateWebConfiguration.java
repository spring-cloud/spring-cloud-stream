/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.EndpointCorsProperties;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.actuate.endpoint.AbstractEndpoint;
import org.springframework.boot.actuate.endpoint.mvc.AbstractEndpointMvcAdapter;
import org.springframework.boot.actuate.endpoint.mvc.AbstractMvcEndpoint;
import org.springframework.boot.actuate.endpoint.mvc.EndpointHandlerMapping;
import org.springframework.boot.actuate.endpoint.mvc.EndpointHandlerMappingCustomizer;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpoint;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpointSecurityInterceptor;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpoints;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.web.BasicErrorController;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.util.CollectionUtils;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.handler.AbstractHandlerMethodMapping;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;

/**
 * Configuration class that sets up web endpoints if web enabled.
 *
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
public class AggregateWebConfiguration {

	@Autowired(required = false)
	private ManagementServerProperties managementServerProperties;

	@Autowired(required = false)
	private List<EndpointHandlerMappingCustomizer> mappingCustomizers;

	@Autowired(required = false)
	private EndpointCorsProperties corsProperties;

	@Bean
	@Lazy
	@ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.mvc.MvcEndpointSecurityInterceptor")
	public EndpointHandlerMapping endpointHandlerMapping(MvcEndpointsHolder mvcEndpointsHolder) {
		EndpointHandlerMapping mapping = null;
		if (this.corsProperties != null) {
			CorsConfiguration corsConfiguration = getCorsConfiguration(this.corsProperties);
			mapping = new EndpointHandlerMapping(mvcEndpointsHolder.getMvcEndpoints(), corsConfiguration);
		}
		else {
			mapping = new EndpointHandlerMapping(mvcEndpointsHolder.getMvcEndpoints(), null);
		}
		if (managementServerProperties != null) {
			mapping.setPrefix(this.managementServerProperties.getContextPath());
			MvcEndpointSecurityInterceptor securityInterceptor = new MvcEndpointSecurityInterceptor(
					this.managementServerProperties.getSecurity().isEnabled(),
					this.managementServerProperties.getSecurity().getRoles());
			mapping.setSecurityInterceptor(securityInterceptor);
		}
		if (this.mappingCustomizers != null) {
			for (EndpointHandlerMappingCustomizer customizer : this.mappingCustomizers) {
				customizer.customize(mapping);
			}
		}
		return mapping;
	}

	@Bean
	@Lazy
	@ConditionalOnMissingClass("org.springframework.boot.actuate.endpoint.mvc.MvcEndpointSecurityInterceptor")
	public EndpointHandlerMapping endpointHandlerMappingWithoutSecurityInterceptor(
			MvcEndpointsHolder mvcEndpointsHolder) {
		EndpointHandlerMapping mapping = null;
		if (this.corsProperties != null) {
			CorsConfiguration corsConfiguration = getCorsConfiguration(this.corsProperties);
			mapping = new EndpointHandlerMapping(mvcEndpointsHolder.getMvcEndpoints(), corsConfiguration);
		}
		else {
			mapping = new EndpointHandlerMapping(mvcEndpointsHolder.getMvcEndpoints(), null);
		}
		if (managementServerProperties != null) {
			mapping.setPrefix(this.managementServerProperties.getContextPath());
		}
		if (this.mappingCustomizers != null) {
			for (EndpointHandlerMappingCustomizer customizer : this.mappingCustomizers) {
				customizer.customize(mapping);
			}
		}
		return mapping;
	}

	private CorsConfiguration getCorsConfiguration(EndpointCorsProperties properties) {
		if (CollectionUtils.isEmpty(properties.getAllowedOrigins())) {
			return null;
		}
		CorsConfiguration configuration = new CorsConfiguration();
		configuration.setAllowedOrigins(properties.getAllowedOrigins());
		if (!CollectionUtils.isEmpty(properties.getAllowedHeaders())) {
			configuration.setAllowedHeaders(properties.getAllowedHeaders());
		}
		if (!CollectionUtils.isEmpty(properties.getAllowedMethods())) {
			configuration.setAllowedMethods(properties.getAllowedMethods());
		}
		if (!CollectionUtils.isEmpty(properties.getExposedHeaders())) {
			configuration.setExposedHeaders(properties.getExposedHeaders());
		}
		if (properties.getMaxAge() != null) {
			configuration.setMaxAge(properties.getMaxAge());
		}
		if (properties.getAllowCredentials() != null) {
			configuration.setAllowCredentials(properties.getAllowCredentials());
		}
		return configuration;
	}

	@Bean
	public WebEndpointConfigurer webEndpointConfigurer() {
		return new WebEndpointConfigurer();
	}

	public class WebEndpointConfigurer implements ApplicationContextAware {

		private ConfigurableApplicationContext applicationContext;

		@Override
		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}

		void exposeWebEndpoints(List<AggregateApplicationBuilder.AppConfigurer<?>> apps) {
			Set<MvcEndpoint> mvcEndpointsToAdd = new HashSet<>();
			Map<RequestMappingInfo, Map<Object, Method>> requestMappingInfo = new HashMap<>();
			for (int i = apps.size() - 1; i >= 0; i--) {
				AggregateApplicationBuilder.AppConfigurer<?> appConfigurer = apps.get(i);
				ConfigurableApplicationContext childContext = appConfigurer.embed();
				// Map the Spring MVC RequestMapping into parent context
				AbstractHandlerMethodMapping handlerMethodMapping = childContext
						.getBean(AbstractHandlerMethodMapping.class);
				Map<RequestMappingInfo, HandlerMethod> mappings = handlerMethodMapping.getHandlerMethods();
				for (Map.Entry<RequestMappingInfo, HandlerMethod> mapping : mappings.entrySet()) {
					Map<Object, Method> handlerMethodMap = new HashMap<>();
					HandlerMethod handlerMethod = mapping.getValue();
					if (!BasicErrorController.class.isAssignableFrom(handlerMethod.getBeanType())) {
						Object handlerBean = handlerMethod.getBean();
						try {
							this.applicationContext.getBeanFactory().getBean(handlerBean.toString());
						}
						catch (NoSuchBeanDefinitionException e) {
							// ChildContext is expected to have the bean as the
							// request mapping is retrieved from the child context.
							this.applicationContext.getBeanFactory().registerSingleton(handlerBean.toString(),
									childContext.getBean(handlerBean.toString()));
						}
						handlerMethodMap.put(handlerBean, handlerMethod.getMethod());
					}
					requestMappingInfo.put(mapping.getKey(), handlerMethodMap);
				}
				// Map the actuator web endpoints
				MvcEndpoints mvcEndpoints = new MvcEndpoints();
				mvcEndpoints.setApplicationContext(childContext);
				try {
					mvcEndpoints.afterPropertiesSet();
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
				for (MvcEndpoint mvcEndpoint : mvcEndpoints.getEndpoints()) {
					String namespaceValue = Pattern.compile("\\w+").matcher(appConfigurer.getNamespace()).matches()
							? appConfigurer.getNamespace() : "child_" + i;
					if (mvcEndpoint instanceof AbstractMvcEndpoint) {
						((AbstractMvcEndpoint) mvcEndpoint)
								.setPath("/" + namespaceValue + "/" + mvcEndpoint.getPath());
					}
					else if (mvcEndpoint instanceof AbstractEndpointMvcAdapter) {
						((AbstractEndpointMvcAdapter) mvcEndpoint)
								.setPath("/" + namespaceValue + "/" + mvcEndpoint.getPath());
					}
					else if (mvcEndpoint instanceof AbstractEndpoint) {
						((AbstractEndpoint) mvcEndpoint)
								.setId(namespaceValue + "_" + ((AbstractEndpoint) mvcEndpoint).getId());
					}
				}
				mvcEndpointsToAdd.addAll(mvcEndpoints.getEndpoints());
			}
			MvcEndpointsHolder mvcEndpointsHolder = new MvcEndpointsHolder(mvcEndpointsToAdd);
			this.applicationContext.getBeanFactory().registerSingleton("mvcEndpointsHolder", mvcEndpointsHolder);
			// invoke lazy bean to get instantiated after setting mvcEndpoints
			EndpointHandlerMapping endpointHandlerMapping = this.applicationContext
					.getBean(EndpointHandlerMapping.class);
			for (Map.Entry<RequestMappingInfo, Map<Object, Method>> mappingInfoMapEntry : requestMappingInfo
					.entrySet()) {
				for (Map.Entry<Object, Method> handlerMethodEntry : mappingInfoMapEntry.getValue().entrySet()) {
					endpointHandlerMapping.registerMapping(mappingInfoMapEntry.getKey(),
							handlerMethodEntry.getKey(), handlerMethodEntry.getValue());
				}
			}
		}
	}

	public class MvcEndpointsHolder {

		private final Set<MvcEndpoint> mvcEndpoints;

		public MvcEndpointsHolder(Set<MvcEndpoint> mvcEndpoints) {
			this.mvcEndpoints = mvcEndpoints;
		}

		public Set<MvcEndpoint> getMvcEndpoints() {
			return mvcEndpoints;
		}
	}
}
