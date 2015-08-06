/*
 * Copyright 2015 the original author or authors.
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
import java.util.HashSet;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 *
 */
@ConfigurationProperties("spring.cloud.streams")
public class AggregateBuilder implements ApplicationContextAware {

	private ConfigurableApplicationContext parent;

	private int index = 0;

	private String streamName = "stream";

	private HashSet<SinkConfigurer> sinks = new HashSet<SinkConfigurer>();

	public String getName() {
		return this.streamName;
	}

	public void setName(String name) {
		this.streamName = name;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.parent = (ConfigurableApplicationContext) applicationContext;
	}

	public void build() {
		for (SinkConfigurer sink : this.sinks) {
			sink.build();
		}
	}

	public SourceConfigurer from(Class<?> module) {
		return new SourceConfigurer(module);
	}

	private String channelName() {
		return this.streamName + "." + this.index;
	}

	private String incrementChannelName() {
		return this.streamName + "." + (this.index++);
	}

	public class SourceConfigurer {

		private Class<?> module;
		private String[] names = null;
		private String[] profiles = null;

		public SourceConfigurer(Class<?> module) {
			this.module = module;
		}

		public SourceConfigurer as(String... names) {
			this.names = names;
			return this;
		}

		public SourceConfigurer profiles(String... profiles) {
			this.profiles = profiles;
			return this;
		}

		public SinkConfigurer to(Class<?> sink) {
			build();
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			build();
			return new ProcessorConfigurer(processor);
		}

		private void build() {
			childContext(this.module).config(this.names).profiles(this.profiles)
			.output(channelName()).build();
		}

	}

	public class SinkConfigurer {

		private Class<?> module;
		private String[] names = null;
		private String[] profiles = null;

		public SinkConfigurer profiles(String... profiles) {
			this.profiles = profiles;
			return this;
		}

		public SinkConfigurer(Class<?> module) {
			AggregateBuilder.this.sinks.add(this);
			this.module = module;
		}

		public SinkConfigurer as(String... names) {
			this.names = names;
			return this;
		}

		void build() {
			childContext(this.module).config(this.names).profiles(this.profiles)
			.input(channelName()).build();
		}

	}

	public class ProcessorConfigurer {

		private Class<?> module;
		private String[] names = null;
		private String[] profiles = null;

		public ProcessorConfigurer(Class<?> module) {
			this.module = module;
		}

		public ProcessorConfigurer as(String... names) {
			this.names = names;
			return this;
		}

		public ProcessorConfigurer profiles(String... profiles) {
			this.profiles = profiles;
			return this;
		}

		public SinkConfigurer to(Class<?> sink) {
			build();
			return new SinkConfigurer(sink);
		}

		public ProcessorConfigurer via(Class<?> processor) {
			build();
			return new ProcessorConfigurer(processor);
		}

		private void build() {
			childContext(this.module).config(this.names).profiles(this.profiles)
			.input(incrementChannelName()).output(channelName()).build();
		}

	}

	private ChildContextBuilder childContext(Class<?> type) {
		return new ChildContextBuilder(new SpringApplicationBuilder(type,
				SeedConfiguration.class).parent(AggregateBuilder.this.parent)
				.showBanner(false).web(false)
				.initializers(new BeanPostProcessorInitializer()));
	}

	private class ChildContextBuilder {

		private SpringApplicationBuilder builder;
		private String configName;
		private String input;
		private String output;

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

		public ChildContextBuilder input(String input) {
			this.input = input;
			return this;
		}

		public ChildContextBuilder output(String output) {
			this.output = output;
			return this;
		}

		public void build() {
			List<String> args = new ArrayList<String>();
			if (this.configName != null) {
				args.add("--spring.config.name=" + this.configName);
			}
			if (this.input != null) {
				args.add("--spring.cloud.stream.bindings.input=" + this.input);
			}
			if (this.output != null) {
				args.add("--spring.cloud.stream.bindings.output=" + this.output);
			}
			this.builder.run(args.toArray(new String[0]));
		}

	}

	private class BeanPostProcessorInitializer implements
	ApplicationContextInitializer<ConfigurableApplicationContext> {

		@Override
		public void initialize(ConfigurableApplicationContext applicationContext) {
		}

	}

	@Configuration
	@EnableAutoConfiguration
	protected static class SeedConfiguration {
	}

}
