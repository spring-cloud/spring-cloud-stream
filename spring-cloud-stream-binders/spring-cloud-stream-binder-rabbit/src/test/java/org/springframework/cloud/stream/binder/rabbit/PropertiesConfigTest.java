/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.net.MalformedURLException;
import java.util.Properties;

import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author David Turanski
 */
public class PropertiesConfigTest {
	@Test
	public void test() {

		SpringApplication application = new SpringApplicationBuilder(MyConfig.class)
				.listeners(new ApplicationListener<ApplicationPreparedEvent>() {
					@Override
					public void onApplicationEvent(ApplicationPreparedEvent applicationEvent) {
						Properties properties = new Properties();
						properties.setProperty("foo.resource", "file:/resource.txt");
						properties.setProperty("foo.strings", "foo,bar");
						properties.setProperty("foo.values", "BAR");
						applicationEvent.getApplicationContext().getEnvironment().getPropertySources().addLast(new
								PropertiesPropertySource("foo", properties));

					}
				})
				.listeners(new ApplicationListener<ApplicationReadyEvent>() {
					@Override
					public void onApplicationEvent(ApplicationReadyEvent applicationEvent) {
						Foo foo = applicationEvent.getApplicationContext().getBean(Foo.class);
						assertEquals(Foo.Values.BAR, foo.getValues());
						assertEquals(new String[] {"foo", "bar"}, foo.getStrings());
						System.out.println(foo.getResource().getClass().getName());
						try {
							assertEquals(new UrlResource("file:/resource.txt"),
									foo.getResource());
						}
						catch (MalformedURLException e) {
							fail(e.getMessage());
						}
					}
				})
				.build();

		application.run();

	}

}

@Configuration
@EnableConfigurationProperties(FooProperties.class)
class MyConfig {

	@Autowired FooProperties fooProperties;

	@Bean
	Foo foo() {
		Foo foo = new Foo();
		foo.setResource(fooProperties.getResource());
		foo.setStrings(fooProperties.getStrings());
		foo.setValues(fooProperties.getValues());
		return foo;
	}
}

class Foo {

	public static enum Values {FOO, BAR}

	Resource resource;

	String[] strings;

	Values values;

	public String[] getStrings() {
		return strings;
	}

	public void setStrings(String[] strings) {
		this.strings = strings;
	}

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public Values getValues() {
		return values;
	}

	public void setValues(Values values) {
		this.values = values;
	}
}


@ConfigurationProperties(prefix = "foo")
class FooProperties {

	Resource resource;

	String[] strings;

	Foo.Values values;

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public String[] getStrings() {
		return strings;
	}

	public void setStrings(String[] strings) {
		this.strings = strings;
	}

	public Foo.Values getValues() {
		return values;
	}

	public void setValues(Foo.Values values) {
		this.values = values;
	}
}
