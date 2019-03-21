/*
 * Copyright 2015-2019 the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationB;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 */
public class BinderConfigurationParsingTests {

	private static ClassLoader classLoader = BinderConfigurationParsingTests.class
			.getClassLoader();

	@Test
	public void testParseOneBinderConfiguration() throws Exception {

		// this is just checking that resources are passed and classes are loaded properly
		// class values used here are not binder configurations
		String oneBinderConfiguration = "binder1=org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration";
		Resource resource = new InputStreamResource(
				new ByteArrayInputStream(oneBinderConfiguration.getBytes()));

		Collection<BinderType> binderConfigurations = BinderFactoryConfiguration
				.parseBinderConfigurations(classLoader, resource);

		assertThat(binderConfigurations).isNotNull();
		assertThat(binderConfigurations.size()).isEqualTo(1);
		BinderType type = binderConfigurations.iterator().next();
		assertThat(type.getDefaultName()).isEqualTo("binder1");
		assertThat(type.getConfigurationClasses())
				.contains(StubBinder1Configuration.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testParseTwoBindersConfigurations() throws Exception {
		// this is just checking that resources are passed and classes are loaded properly
		// class values used here are not binder configurations
		String binderConfiguration = "binder1=org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration\n"
				+ "binder2=org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA";
		Resource twoBinderConfigurationResource = new InputStreamResource(
				new ByteArrayInputStream(binderConfiguration.getBytes()));

		Collection<BinderType> twoBinderConfig = BinderFactoryConfiguration
				.parseBinderConfigurations(classLoader, twoBinderConfigurationResource);

		assertThat(twoBinderConfig.size()).isEqualTo(2);
		List<BinderType> stubBinder1 = stubBinders(twoBinderConfig, "binder1",
				StubBinder1Configuration.class);
		List<BinderType> stubBinder2 = stubBinders(twoBinderConfig, "binder2",
				StubBinder2ConfigurationA.class);
		assertThat(stubBinder1).isNotEmpty();
		assertThat(stubBinder2).isNotEmpty();

	}

	private List<BinderType> stubBinders(Collection<BinderType> twoBinderConfigurations,
			String binderName, Class... configurationNames) {
		return twoBinderConfigurations.stream()
				.filter(binderType -> binderName.equals(binderType.getDefaultName())
						&& !Collections.disjoint(
								Arrays.asList(binderType.getConfigurationClasses()),
								Arrays.asList(configurationNames)))
				.collect(Collectors.toList());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParseTwoBindersWithMultipleClasses() throws Exception {
		// this is just checking that resources are passed and classes are loaded properly
		// class values used here are not binder configurations
		String binderConfiguration = "binder1=org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration\n"
				+ "binder2=org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA,"
				+ "org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationB";
		Resource binderConfigurationResource = new InputStreamResource(
				new ByteArrayInputStream(binderConfiguration.getBytes()));

		Collection<BinderType> binderConfigurations = BinderFactoryConfiguration
				.parseBinderConfigurations(classLoader, binderConfigurationResource);

		assertThat(binderConfigurations.size()).isEqualTo(2);
		assertThat(binderConfigurations.size()).isEqualTo(2);
		List<BinderType> stubBinder1 = stubBinders(binderConfigurations, "binder1",
				StubBinder1Configuration.class);
		List<BinderType> stubBinder2 = stubBinders(binderConfigurations, "binder2",
				StubBinder2ConfigurationA.class, StubBinder2ConfigurationB.class);
		assertThat(stubBinder1).isNotEmpty();
		assertThat(stubBinder2).isNotEmpty();
	}

}
