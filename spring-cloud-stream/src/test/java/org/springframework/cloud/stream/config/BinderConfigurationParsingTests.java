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

package org.springframework.cloud.stream.config;

import java.io.ByteArrayInputStream;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA;
import org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationB;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasProperty;

/**
 * @author Marius Bogoevici
 */
public class BinderConfigurationParsingTests {

	private static ClassLoader classLoader = BinderConfigurationParsingTests.class.getClassLoader();

	@Test
	public void testParseOneBinderConfiguration() throws Exception {

		// this is just checking that resources are passed and classes are loaded properly
		// class values used here are not binder configurations
		String oneBinderConfiguration = "binder1=org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration";
		Resource resource = new InputStreamResource(new ByteArrayInputStream(oneBinderConfiguration.getBytes()));

		Collection<BinderType> binderConfigurations = BinderFactoryConfiguration.parseBinderConfigurations(classLoader,
				resource);

		Assert.assertNotNull(binderConfigurations);
		Assert.assertThat(binderConfigurations.size(), equalTo(1));
		Assert.assertThat(binderConfigurations, contains(
				both(hasProperty("defaultName", equalTo("binder1"))).and(
						hasProperty("configurationClasses", hasItemInArray(StubBinder1Configuration.class)))));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testParseTwoBindersConfigurations() throws Exception {
		// this is just checking that resources are passed and classes are loaded properly
		// class values used here are not binder configurations
		String binderConfiguration = "binder1=org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration\n"
				+
				"binder2=org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA";
		Resource twoBinderConfigurationResource = new InputStreamResource(
				new ByteArrayInputStream(binderConfiguration.getBytes()));

		Collection<BinderType> twoBinderConfigurations = BinderFactoryConfiguration.parseBinderConfigurations(
				classLoader,
				twoBinderConfigurationResource);

		Assert.assertThat(twoBinderConfigurations.size(), equalTo(2));
		Assert.assertThat(twoBinderConfigurations, containsInAnyOrder(
				both(hasProperty("defaultName", equalTo("binder1"))).and(
						hasProperty("configurationClasses", hasItemInArray(StubBinder1Configuration.class))),
				both(hasProperty("defaultName", equalTo("binder2"))).and(
						hasProperty("configurationClasses", hasItemInArray(StubBinder2ConfigurationA.class)))));

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParseTwoBindersWithMultipleClasses() throws Exception {
		// this is just checking that resources are passed and classes are loaded properly
		// class values used here are not binder configurations
		String binderConfiguration = "binder1=org.springframework.cloud.stream.binder.stub1.StubBinder1Configuration\n"
				+
				"binder2=org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationA," +
				"org.springframework.cloud.stream.binder.stub2.StubBinder2ConfigurationB";
		Resource binderConfigurationResource = new InputStreamResource(
				new ByteArrayInputStream(binderConfiguration.getBytes()));

		Collection<BinderType> binderConfigurations = BinderFactoryConfiguration.parseBinderConfigurations(classLoader,
				binderConfigurationResource);

		Assert.assertThat(binderConfigurations.size(), equalTo(2));
		Assert.assertThat(binderConfigurations, containsInAnyOrder(
				both(hasProperty("defaultName", equalTo("binder1"))).and(
						hasProperty("configurationClasses", hasItemInArray(StubBinder1Configuration.class))),
				both(hasProperty("defaultName", equalTo("binder2"))).and(
						hasProperty("configurationClasses", arrayContainingInAnyOrder(StubBinder2ConfigurationA.class,
								StubBinder2ConfigurationB.class)))));

	}
}
