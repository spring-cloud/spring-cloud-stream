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
import java.util.List;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.StringUtils;

/**
 * Application builder for {@link AggregateApplication}.
 *
 * @author Dave Syer
 * @author Ilayaperumal Gopinathan
 *
 */
public class AggregateApplicationBuilder {

	private SourceConfigurer sourceConfigurer;

	private SinkConfigurer sinkConfigurer;

	private List<ProcessorConfigurer> processorConfigurers = new ArrayList<>();

	private AggregateApplicationBuilder applicationBuilder = this;

	public SourceConfigurer from(Class<?> app) {
		SourceConfigurer sourceConfigurer = new SourceConfigurer(app);
		this.sourceConfigurer = sourceConfigurer;
		return sourceConfigurer;
	}

	public void run(String[] parentArgs) {
		ConfigurableApplicationContext parentContext = AggregateApplication.createParentContext(parentArgs);
		SharedChannelRegistry sharedChannelRegistry = parentContext.getBean(SharedChannelRegistry.class);
		List<AppConfigurer> apps = new ArrayList<AppConfigurer>();
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
		List<Class<?>> appsToEmbed = new ArrayList<>();
		for (int i = 0; i < apps.size(); i++) {
			appsToEmbed.add(apps.get(i).getApp());
		}
		AggregateApplication.prepareSharedChannelRegistry(sharedChannelRegistry, appsToEmbed.toArray(new Class<?>[0]));
		for (int i = apps.size() - 1; i >= 0; i--) {
			AppConfigurer appConfigurer = apps.get(i);
			appConfigurer.setParentContext(parentContext);
			appConfigurer.setNamespace(AggregateApplication.getNamespace(appConfigurer.getApp().getName(), i));
			appConfigurer.embed();
		}
	}


	public class SourceConfigurer extends AppConfigurer {

		public SourceConfigurer(Class<?> app) {
			this.app = app;
			sourceConfigurer = this;
		}

		public SourceConfigurer as(String... names) {
			this.names = names;
			return this;
		}

		public SourceConfigurer args(String... args) {
			this.args = args;
			return this;
		}

		public SourceConfigurer profiles(String... profiles) {
			this.profiles = profiles;
			return this;
		}

		public SinkConfigurer to(Class<?> sink) {
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			return new ProcessorConfigurer(processor);
		}

	}

	public class SinkConfigurer extends AppConfigurer {

		public SinkConfigurer(Class<?> app) {
			this.app = app;
			sinkConfigurer = this;
		}

		public SinkConfigurer as(String... names) {
			this.names = names;
			return this;
		}

		public SinkConfigurer args(String... args) {
			this.args = args;
			return this;
		}

		public SinkConfigurer profiles(String... profiles) {
			this.profiles = profiles;
			return this;
		}

	}

	public class ProcessorConfigurer extends AppConfigurer {

		public ProcessorConfigurer(Class<?> app) {
			this.app = app;
			processorConfigurers.add(this);
		}

		public ProcessorConfigurer as(String... names) {
			this.names = names;
			return this;
		}

		public ProcessorConfigurer args(String... args) {
			this.args = args;
			return this;
		}

		public ProcessorConfigurer profiles(String... profiles) {
			this.profiles = profiles;
			return this;
		}

		public SinkConfigurer to(Class<?> sink) {
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			return new ProcessorConfigurer(processor);
		}

	}

	private abstract class AppConfigurer {

		Class<?> app;

		String[] args;

		String[] names = null;

		String[] profiles = null;

		ConfigurableApplicationContext parentContext;

		String namespace;

		Class<?> getApp() {
			return this.app;
		}

		public void setParentContext(ConfigurableApplicationContext parentContext) {
			this.parentContext = parentContext;
		}

		public void setNamespace(String namespace) {
			this.namespace = namespace;
		}

		public void run(String... args) {
			applicationBuilder.run(args);
		}

		void embed() {
			childContext(this.app, this.parentContext, this.namespace).args(this.args).config(this.names).profiles(this.profiles).run();
		}
	}

	private ChildContextBuilder childContext(Class<?> app, ConfigurableApplicationContext parentContext, String namespace) {
		return new ChildContextBuilder(AggregateApplication.embedApp(parentContext, namespace, app));
	}

	private class ChildContextBuilder {

		private SpringApplicationBuilder builder;

		private String configName;

		private String[] args;

		public ChildContextBuilder(SpringApplicationBuilder builder) {
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

}
