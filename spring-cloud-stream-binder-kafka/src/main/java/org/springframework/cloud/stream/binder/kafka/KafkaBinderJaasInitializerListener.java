/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.kafka.common.security.JaasUtils;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class KafkaBinderJaasInitializerListener implements ApplicationListener<ContextRefreshedEvent>,
		ApplicationContextAware, DisposableBean {

	public static final String DEFAULT_ZK_LOGIN_CONTEXT_NAME = "Client";

	private ApplicationContext applicationContext;

	private final boolean ignoreJavaLoginConfigParamSystemProperty;

	private final File placeholderJaasConfiguration;

	public KafkaBinderJaasInitializerListener() throws IOException {
		// we ignore the system property if it wasn't originally set at launch
		this.ignoreJavaLoginConfigParamSystemProperty =
				(System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM) == null);
		this.placeholderJaasConfiguration = File.createTempFile("kafka-client-jaas-config-placeholder", "conf");
		this.placeholderJaasConfiguration.deleteOnExit();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void destroy() throws Exception {
		if (this.ignoreJavaLoginConfigParamSystemProperty) {
			System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
		}
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getSource() == this.applicationContext) {
			KafkaBinderConfigurationProperties binderConfigurationProperties =
					applicationContext.getBean(KafkaBinderConfigurationProperties.class);
			// only use programmatic support if a file is not set via system property
			if (ignoreJavaLoginConfigParamSystemProperty
					&& binderConfigurationProperties.getJaas() != null) {
				Map<String, AppConfigurationEntry[]> configurationEntries = new HashMap<>();
				AppConfigurationEntry kafkaClientConfigurationEntry = new AppConfigurationEntry
						(binderConfigurationProperties.getJaas().getLoginModule(),
								binderConfigurationProperties.getJaas().getControlFlagValue(),
								binderConfigurationProperties.getJaas().getOptions() != null ?
										binderConfigurationProperties.getJaas().getOptions() :
										Collections.<String, Object>emptyMap());
				configurationEntries.put(JaasUtils.LOGIN_CONTEXT_CLIENT,
						new AppConfigurationEntry[]{ kafkaClientConfigurationEntry });
				Configuration.setConfiguration(new InternalConfiguration(configurationEntries));
				// Workaround for a 0.9 client issue where even if the Configuration is set
				// a system property check is performed.
				// Since the Configuration already exists, this will be ignored.
				if (this.placeholderJaasConfiguration != null) {
					System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, this.placeholderJaasConfiguration.getAbsolutePath());
				}
			}
		}
	}

	/**
	 * A {@link Configuration} set up programmatically by the Kafka binder
	 */
	public static class InternalConfiguration extends Configuration {

		private final Map<String, AppConfigurationEntry[]> configurationEntries;

		public InternalConfiguration(Map<String, AppConfigurationEntry[]> configurationEntries) {
			Assert.notNull(configurationEntries, " cannot be null");
			Assert.notEmpty(configurationEntries, " cannot be empty");
			this.configurationEntries = configurationEntries;
		}

		@Override
		public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
			return configurationEntries.get(name);
		}
	}
}
