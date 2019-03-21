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

package org.springframework.cloud.stream.aggregate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.ChannelBindingAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Application builder for {@link AggregateApplication}.
 *
 * @author Dave Syer
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Venil Noronha
 * @author Janne Valkealahti
 * @author Vinicius Carvalho
 * @author Oleg Zhurakousky
 */
@EnableBinding
public class AggregateApplicationBuilder implements AggregateApplication,
		ApplicationContextAware, SmartInitializingSingleton {

	private static final String CHILD_CONTEXT_SUFFIX = ".spring.cloud.stream.context";

	private static final Bindable<Map<String, String>> STRING_STRING_MAP = Bindable
			.mapOf(String.class, String.class);

	private static final Pattern DOLLAR_ESCAPE_PATTERN = Pattern.compile("\\$");

	private SourceConfigurer sourceConfigurer;

	private SinkConfigurer sinkConfigurer;

	private List<ProcessorConfigurer> processorConfigurers = new ArrayList<>();

	private AggregateApplicationBuilder applicationBuilder = this;

	private ConfigurableApplicationContext parentContext;

	private List<Object> parentSources = new ArrayList<>();

	private List<String> parentArgs = new ArrayList<>();

	private boolean headless = true;

	private boolean webEnvironment = true;

	public AggregateApplicationBuilder(String... args) {
		this(new Object[] { ParentConfiguration.class }, args);
	}

	public AggregateApplicationBuilder(Object source, String... args) {
		this(new Object[] { source }, args);
	}

	public AggregateApplicationBuilder(Object[] sources, String[] args) {
		addParentSources(sources);
		this.parentArgs.addAll(Arrays.asList(args));
	}

	/**
	 * Adding auto configuration classes to parent sources excluding the configuration
	 * classes related to binder/binding.
	 * @param sources sources to which parent sources will be added
	 */
	private void addParentSources(Object[] sources) {
		if (!this.parentSources.contains(ParentConfiguration.class)) {
			this.parentSources.add(ParentConfiguration.class);
			if (ClassUtils.isPresent(
					"org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration",
					null)) {
				this.parentSources.add(ParentActuatorConfiguration.class);
			}
		}
		this.parentSources.addAll(Arrays.asList(sources));
	}

	public AggregateApplicationBuilder parent(Object source, String... args) {
		return parent(new Object[] { source }, args);
	}

	public AggregateApplicationBuilder parent(Object[] sources, String[] args) {
		addParentSources(sources);
		this.parentArgs.addAll(Arrays.asList(args));
		return this;
	}

	/**
	 * Flag to explicitly request a web or non-web environment.
	 * @param webEnvironment true if the application has a web environment
	 * @return the AggregateApplicationBuilder being constructed
	 * @see SpringApplicationBuilder#web(boolean)
	 */
	public AggregateApplicationBuilder web(boolean webEnvironment) {
		this.webEnvironment = webEnvironment;
		return this;
	}

	/**
	 * Configures the headless attribute of the build application.
	 * @param headless true if the application is headless
	 * @return the AggregateApplicationBuilder being constructed
	 * @see SpringApplicationBuilder#headless(boolean)
	 */
	public AggregateApplicationBuilder headless(boolean headless) {
		this.headless = headless;
		return this;
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.run();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.parentContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public <T> T getBinding(Class<T> bindableType, String namespace) {
		if (this.parentContext == null) {
			throw new IllegalStateException(
					"The aggregate application has not been started yet");
		}
		try {
			ChildContextHolder contextHolder = this.parentContext
					.getBean(namespace + CHILD_CONTEXT_SUFFIX, ChildContextHolder.class);
			return contextHolder.getChildContext().getBean(bindableType);
		}
		catch (BeansException e) {
			throw new IllegalStateException("Binding not found for '"
					+ bindableType.getName() + "' into namespace " + namespace);
		}
	}

	public SourceConfigurer from(Class<?> app) {
		SourceConfigurer sourceConfigurer = new SourceConfigurer(app);
		this.sourceConfigurer = sourceConfigurer;
		return sourceConfigurer;
	}

	public ConfigurableApplicationContext run(String... parentArgs) {
		this.parentArgs.addAll(Arrays.asList(parentArgs));
		List<AppConfigurer<?>> apps = new ArrayList<>();
		if (this.sourceConfigurer != null) {
			apps.add(this.sourceConfigurer);
		}
		if (!this.processorConfigurers.isEmpty()) {
			for (ProcessorConfigurer processorConfigurer : this.processorConfigurers) {
				apps.add(processorConfigurer);
			}
		}
		if (this.sinkConfigurer != null) {
			apps.add(this.sinkConfigurer);
		}
		LinkedHashMap<Class<?>, String> appsToEmbed = new LinkedHashMap<>();
		LinkedHashMap<AppConfigurer<?>, String> appConfigurers = new LinkedHashMap<>();
		for (int i = 0; i < apps.size(); i++) {
			AppConfigurer<?> appConfigurer = apps.get(i);
			Class<?> appToEmbed = appConfigurer.getApp();
			// Always update namespace before preparing SharedChannelRegistry
			if (appConfigurer.namespace == null) {
				// to remove illegal characters for new properties
				// binder
				// org.springframework.cloud.stream.aggregation.AggregationTest$TestSource
				appConfigurer.namespace = AggregateApplicationUtils.getDefaultNamespace(
						DOLLAR_ESCAPE_PATTERN.matcher(appConfigurer.getApp().getName())
								.replaceAll("."),
						i);
			}
			appsToEmbed.put(appToEmbed, appConfigurer.namespace);
			appConfigurers.put(appConfigurer, appConfigurer.namespace);
		}
		if (this.parentContext == null) {
			if (Boolean.TRUE.equals(this.webEnvironment)) {
				Assert.isTrue(
						ClassUtils.isPresent("javax.servlet.ServletRequest",
								ClassUtils.getDefaultClassLoader()),
						"'webEnvironment' is set to 'true' but 'javax.servlet.*' does not appear to be available in "
								+ "the classpath. Consider adding `org.springframework.boot:spring-boot-starter-web");
				this.addParentSources(
						new Object[] { ServletWebServerFactoryAutoConfiguration.class });
			}
			this.parentContext = AggregateApplicationUtils.createParentContext(
					this.parentSources.toArray(new Class<?>[0]),
					this.parentArgs.toArray(new String[0]), selfContained(),
					this.webEnvironment, this.headless);
		}
		else {
			if (BeanFactoryUtils.beansOfTypeIncludingAncestors(this.parentContext,
					SharedBindingTargetRegistry.class).size() == 0) {
				SharedBindingTargetRegistry sharedBindingTargetRegistry = new SharedBindingTargetRegistry();
				this.parentContext.getBeanFactory().registerSingleton(
						"sharedBindingTargetRegistry", sharedBindingTargetRegistry);
			}
		}
		SharedBindingTargetRegistry sharedBindingTargetRegistry = this.parentContext
				.getBean(SharedBindingTargetRegistry.class);
		AggregateApplicationUtils.prepareSharedBindingTargetRegistry(
				sharedBindingTargetRegistry, appsToEmbed);
		for (Map.Entry<AppConfigurer<?>, String> appConfigurerEntry : appConfigurers
				.entrySet()) {

			AppConfigurer<?> appConfigurer = appConfigurerEntry.getKey();
			if (appConfigurerEntry.getValue() == null) {
				continue;
			}
			String namespace = appConfigurerEntry.getValue().toLowerCase();
			Set<String> argsToUpdate = new LinkedHashSet<>();
			Set<String> argKeys = new LinkedHashSet<>();
			Map<String, String> target = bindProperties(namespace,
					this.parentContext.getEnvironment());

			if (!target.isEmpty()) {
				for (Map.Entry<String, String> entry : target.entrySet()) {
					String key = entry.getKey();
					argKeys.add(key);
					argsToUpdate.add("--" + key + "=" + entry.getValue());
				}
			}

			if (!argsToUpdate.isEmpty()) {
				appConfigurer.args(argsToUpdate.toArray(new String[0]));
			}
		}
		for (int i = apps.size() - 1; i >= 0; i--) {
			AppConfigurer<?> appConfigurer = apps.get(i);
			appConfigurer.embed();
		}
		if (BeanFactoryUtils.beansOfTypeIncludingAncestors(this.parentContext,
				AggregateApplication.class).size() == 0) {
			this.parentContext.getBeanFactory()
					.registerSingleton("aggregateApplicationAccessor", this);
		}
		return this.parentContext;
	}

	private boolean selfContained() {
		return (this.sourceConfigurer != null) && (this.sinkConfigurer != null);
	}

	private ChildContextBuilder childContext(Class<?> app,
			ConfigurableApplicationContext parentContext, String namespace) {
		return new ChildContextBuilder(
				AggregateApplicationUtils.embedApp(parentContext, namespace, app));
	}

	private Map<String, String> bindProperties(String namepace, Environment environment) {
		Map<String, String> target;
		BindResult<Map<String, String>> bindResult = Binder.get(environment)
				.bind(namepace, STRING_STRING_MAP);
		if (bindResult.isBound()) {
			target = bindResult.get();
		}
		else {
			target = new HashMap<>();
		}
		return target;
	}

	private static class ChildContextHolder {

		private final ConfigurableApplicationContext childContext;

		ChildContextHolder(ConfigurableApplicationContext childContext) {
			Assert.notNull(childContext, "cannot be null");
			this.childContext = childContext;
		}

		public ConfigurableApplicationContext getChildContext() {
			return this.childContext;
		}

	}

	/**
	 * Auto configuration for {@link SharedBindingTargetRegistry}.
	 */
	@ImportAutoConfiguration(ChannelBindingAutoConfiguration.class)
	@EnableBinding
	public static class ParentConfiguration {

		@Bean
		@ConditionalOnMissingBean(SharedBindingTargetRegistry.class)
		public SharedBindingTargetRegistry sharedBindingTargetRegistry() {
			return new SharedBindingTargetRegistry();
		}

	}

	/**
	 * Auto configuration for {@link EndpointAutoConfiguration}.
	 */
	@ImportAutoConfiguration(EndpointAutoConfiguration.class)
	public static class ParentActuatorConfiguration {

	}

	/**
	 * Source configurer.
	 */
	public class SourceConfigurer extends AppConfigurer<SourceConfigurer> {

		public SourceConfigurer(Class<?> app) {
			this.app = app;
			AggregateApplicationBuilder.this.sourceConfigurer = this;
		}

		public SinkConfigurer to(Class<?> sink) {
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			return new ProcessorConfigurer(processor);
		}

	}

	/**
	 * Sink configurer.
	 */
	public class SinkConfigurer extends AppConfigurer<SinkConfigurer> {

		public SinkConfigurer(Class<?> app) {
			this.app = app;
			AggregateApplicationBuilder.this.sinkConfigurer = this;
		}

	}

	/**
	 * Processor configurer.
	 */
	public class ProcessorConfigurer extends AppConfigurer<ProcessorConfigurer> {

		public ProcessorConfigurer(Class<?> app) {
			this.app = app;
			AggregateApplicationBuilder.this.processorConfigurers.add(this);
		}

		public SinkConfigurer to(Class<?> sink) {
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			return new ProcessorConfigurer(processor);
		}

	}

	/**
	 * Abstraction over configuration of an applciation.
	 *
	 * @param <T> type of a configurer
	 */
	public abstract class AppConfigurer<T extends AppConfigurer<T>> {

		Class<?> app;

		String[] args;

		String[] names;

		String[] profiles;

		String namespace;

		Class<?> getApp() {
			return this.app;
		}

		public T as(String... names) {
			this.names = names;
			return getConfigurer();
		}

		public T args(String... args) {
			this.args = args;
			return getConfigurer();
		}

		public T profiles(String... profiles) {
			this.profiles = profiles;
			return getConfigurer();
		}

		@SuppressWarnings("unchecked")
		private T getConfigurer() {
			return (T) this;
		}

		public T namespace(String namespace) {
			this.namespace = namespace;
			return getConfigurer();
		}

		public ConfigurableApplicationContext run(String... args) {
			return AggregateApplicationBuilder.this.applicationBuilder.run(args);
		}

		void embed() {
			final ConfigurableApplicationContext childContext = childContext(this.app,
					AggregateApplicationBuilder.this.parentContext, this.namespace)
							.args(this.args).config(this.names).profiles(this.profiles)
							.run();
			// Register bindable proxies as beans so they can be queried for later
			Map<String, BindableProxyFactory> bindableProxies = BeanFactoryUtils
					.beansOfTypeIncludingAncestors(childContext.getBeanFactory(),
							BindableProxyFactory.class);
			for (String bindableProxyName : bindableProxies.keySet()) {
				try {
					AggregateApplicationBuilder.this.parentContext.getBeanFactory()
							.registerSingleton(this.getNamespace() + CHILD_CONTEXT_SUFFIX,
									new ChildContextHolder(childContext));
				}
				catch (Exception e) {
					throw new IllegalStateException(
							"Error while trying to register the aggregate bound interface '"
									+ bindableProxyName + "' into namespace '"
									+ this.getNamespace() + "'",
							e);
				}
			}

		}

		public AggregateApplication build() {
			return AggregateApplicationBuilder.this.applicationBuilder;
		}

		public String[] getArgs() {
			return this.args;
		}

		public String getNamespace() {
			return this.namespace;
		}

	}

	private final class ChildContextBuilder {

		private SpringApplicationBuilder builder;

		private String configName;

		private String[] args;

		private ChildContextBuilder(SpringApplicationBuilder builder) {
			this.builder = builder;
		}

		public ChildContextBuilder profiles(String... profiles) {
			if (profiles != null) {
				this.builder.profiles(profiles);
			}
			return this;
		}

		public ChildContextBuilder config(String... configs) {
			if (configs != null) {
				this.configName = StringUtils.arrayToCommaDelimitedString(configs);
			}
			return this;
		}

		public ChildContextBuilder args(String... args) {
			this.args = args;
			return this;
		}

		public ConfigurableApplicationContext run() {
			List<String> args = new ArrayList<String>();
			if (this.args != null) {
				args.addAll(Arrays.asList(this.args));
			}
			if (this.configName != null) {
				args.add("--spring.config.name=" + this.configName);
			}
			return this.builder.run(args.toArray(new String[0]));
		}

	}

}
