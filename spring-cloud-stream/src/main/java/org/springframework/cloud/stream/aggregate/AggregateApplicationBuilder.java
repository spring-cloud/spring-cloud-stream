/*
 * Copyright 2015-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Application builder for {@link AggregateApplication}.
 *
 * @author Dave Syer
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Venil Noronha
 */
public class AggregateApplicationBuilder {

	private SourceConfigurer sourceConfigurer;

	private SinkConfigurer sinkConfigurer;

	private List<ProcessorConfigurer> processorConfigurers = new ArrayList<>();

	private AggregateApplicationBuilder applicationBuilder = this;

	private ConfigurableApplicationContext parentContext;

	public AggregateApplicationBuilder(Object source, String... args) {
		this(new Object[]{source}, args);
	}

	public AggregateApplicationBuilder(Object[] sources, String[] args) {
		this(SpringApplication.run(addParentConfiguration(sources), args));
	}

	public AggregateApplicationBuilder(ConfigurableApplicationContext parentContext) {
		this.parentContext = parentContext;
	}

	/**
	 * Adding auto configuration classes to parent sources excluding the configuration
	 * classes related to binder/binding.
	 */
	private static Object[] addParentConfiguration(Object[] sources) {
		Object[] parentSources;
		if (!ObjectUtils.containsElement(sources, ParentConfiguration.class)) {
			// add the ParentConfiguration first, so it can be overridden
			List<Object> sourceList = new ArrayList<>(Arrays.asList(sources));
			sourceList.add(0, ParentConfiguration.class);
			parentSources = sourceList.toArray(new Object[sourceList.size()]);
		}
		else {
			parentSources = sources;
		}
		return parentSources;
	}


	public AggregateApplicationBuilder parent(Object source, String... args) {
		return parent(new Object[]{source}, args);
	}

	public AggregateApplicationBuilder parent(Object[] sources, String[] args) {
		return parent(SpringApplication.run(sources, args));
	}

	public AggregateApplicationBuilder parent(ConfigurableApplicationContext parentContext) {
		Assert.isNull(this.parentContext, "A parent context has already been set");
		this.parentContext = parentContext;
		return this;
	}

	public SourceConfigurer from(Class<?> app) {
		SourceConfigurer sourceConfigurer = new SourceConfigurer(app);
		this.sourceConfigurer = sourceConfigurer;
		return sourceConfigurer;
	}

	public ConfigurableApplicationContext run(String[] parentArgs) {
		List<AppConfigurer<?>> apps = new ArrayList<AppConfigurer<?>>();
		if (this.sourceConfigurer != null) {
			apps.add(sourceConfigurer);
		}
		if (!processorConfigurers.isEmpty()) {
			for (ProcessorConfigurer processorConfigurer : processorConfigurers) {
				apps.add(processorConfigurer);
			}
		}
		if (this.sinkConfigurer != null) {
			apps.add(sinkConfigurer);
		}
		LinkedHashMap<Class<?>, String> appsToEmbed = new LinkedHashMap<>();
		for (int i = 0; i < apps.size(); i++) {
			AppConfigurer<?> appConfigurer = apps.get(i);
			Class<?> appToEmbed = appConfigurer.getApp();
			// Always update namespace before preparing SharedChannelRegistry
			if (appConfigurer.namespace == null) {
				appConfigurer.namespace = AggregateApplication.getNamespace(appConfigurer.getApp().getName(), i);
			}
			appsToEmbed.put(appToEmbed, appConfigurer.namespace);
		}
		if (this.parentContext == null) {
			this.parentContext = AggregateApplication.createParentContext(parentArgs, areAppsSelfContained());
		}
		// make sure to use the parent context that has aggregator parent configuration
		else if (this.parentContext.getBeansOfType(SharedChannelRegistry.class).isEmpty()) {
			this.parentContext = AggregateApplication.createParentContext(this.parentContext, parentArgs, areAppsSelfContained());
		}
		SharedChannelRegistry sharedChannelRegistry = this.parentContext.getBean(SharedChannelRegistry.class);
		AggregateApplication.prepareSharedChannelRegistry(sharedChannelRegistry, appsToEmbed);
		for (int i = apps.size() - 1; i >= 0; i--) {
			AppConfigurer<?> appConfigurer = apps.get(i);
			appConfigurer.embed();
		}
		return this.parentContext;
	}

	private boolean areAppsSelfContained() {
		return (this.sourceConfigurer != null) && (this.sinkConfigurer != null);
	}

	private ChildContextBuilder childContext(Class<?> app,
			ConfigurableApplicationContext parentContext, String namespace) {
		return new ChildContextBuilder(
				AggregateApplication.embedApp(parentContext, namespace, app));
	}


	public class SourceConfigurer extends AppConfigurer<SourceConfigurer> {

		public SourceConfigurer(Class<?> app) {
			this.app = app;
			sourceConfigurer = this;
		}

		public SinkConfigurer to(Class<?> sink) {
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			return new ProcessorConfigurer(processor);
		}

	}

	public class SinkConfigurer extends AppConfigurer<SinkConfigurer> {

		public SinkConfigurer(Class<?> app) {
			this.app = app;
			sinkConfigurer = this;
		}

	}

	public class ProcessorConfigurer extends AppConfigurer<ProcessorConfigurer> {

		public ProcessorConfigurer(Class<?> app) {
			this.app = app;
			processorConfigurers.add(this);
		}

		public SinkConfigurer to(Class<?> sink) {
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			return new ProcessorConfigurer(processor);
		}

	}

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
			return applicationBuilder.run(args);
		}

		void embed() {
			childContext(this.app, AggregateApplicationBuilder.this.parentContext,
					this.namespace).args(this.args).config(this.names)
					.profiles(this.profiles).run();
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

		public void run() {
			List<String> args = new ArrayList<String>();
			if (this.args != null) {
				args.addAll(Arrays.asList(this.args));
			}
			if (this.configName != null) {
				args.add("--spring.config.name=" + this.configName);
			}
			this.builder.run(args.toArray(new String[0]));
		}

	}

	@EnableAutoConfiguration
	public static class ParentConfiguration {
	}

}
