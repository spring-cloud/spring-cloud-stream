/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.streams.utils;

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.streams.annotation.Input;
import org.springframework.cloud.streams.annotation.Output;
import org.springframework.cloud.streams.config.DirectChannelFactoryBean;

/**
 * Utility class for registering bean definitions.
 *
 * @author Marius Bogoevici
 */
public abstract class BeanDefinitionRegistryUtils {

	public static void registerInputChannelBeanDefinition(String name, BeanDefinitionRegistry registry) {
		registerChannelBeanDefinition(Input.class, name, registry);
	}

	public static void registerOutputChannelBeanDefinition(String name, BeanDefinitionRegistry registry) {
		registerChannelBeanDefinition(Output.class, name, registry);
	}

	private static void registerChannelBeanDefinition(Class<? extends Annotation> qualifier, String name, BeanDefinitionRegistry registry) {
		RootBeanDefinition rootBeanDefinition = new RootBeanDefinition(DirectChannelFactoryBean.class);
		rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(qualifier));
		registry.registerBeanDefinition(name, rootBeanDefinition);
	}

}
