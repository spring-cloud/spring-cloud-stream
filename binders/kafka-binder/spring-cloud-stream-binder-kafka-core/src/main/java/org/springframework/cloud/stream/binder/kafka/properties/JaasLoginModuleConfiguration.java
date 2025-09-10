/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.properties;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;
import org.springframework.util.Assert;

/**
 * Contains properties for setting up an {@link AppConfigurationEntry} that can be used
 * for the Kafka or Zookeeper client.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class JaasLoginModuleConfiguration {

	private String loginModule = "com.sun.security.auth.module.Krb5LoginModule";

	private KafkaJaasLoginModuleInitializer.ControlFlag controlFlag = KafkaJaasLoginModuleInitializer.ControlFlag.REQUIRED;

	private Map<String, String> options = new HashMap<>();

	public String getLoginModule() {
		return this.loginModule;
	}

	public void setLoginModule(String loginModule) {
		Assert.notNull(loginModule, "cannot be null");
		this.loginModule = loginModule;
	}

	public KafkaJaasLoginModuleInitializer.ControlFlag getControlFlag() {
		return this.controlFlag;
	}

	public void setControlFlag(String controlFlag) {
		Assert.notNull(controlFlag, "cannot be null");
		this.controlFlag = KafkaJaasLoginModuleInitializer.ControlFlag
				.valueOf(controlFlag.toUpperCase(Locale.ROOT));
	}

	public Map<String, String> getOptions() {
		return this.options;
	}

	public void setOptions(Map<String, String> options) {
		this.options = options;
	}

}
