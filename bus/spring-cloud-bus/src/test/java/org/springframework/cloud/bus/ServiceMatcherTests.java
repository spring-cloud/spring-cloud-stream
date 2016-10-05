/*
 * Copyright 2012-2015 the original author or authors.
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

package org.springframework.cloud.bus;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.util.AntPathMatcher;

/**
 * @author Dave Syer
 *
 */
public class ServiceMatcherTests {

	private ServiceMatcher matcher = new ServiceMatcher();
	private StaticApplicationContext context = new StaticApplicationContext();

	@Before
	public void init() {
		context.setId("one:two:8888");
		context.refresh();
		matcher.setMatcher(new AntPathMatcher(":"));
		matcher.setApplicationContext(context);
	}

	@Test
	public void fromSelf() {
		assertThat(
				matcher.isFromSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "one:two:8888",
								"foo:bar:spam", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelf() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"one:two:8888", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithWildcard() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"one:two:*", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithGlobalWildcard() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"**", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithWildcardName() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"o*", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithWildcardNameAndProfile() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"o*:t*", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithWildcardString() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"o*", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void notForSelfWithWildCardNameAndMismatchingProfile() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"o*:f*", Collections.<String, String>emptyMap())),
				is(false));
	}

	@Test
	public void forSelfWithDoubleWildcard() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"one:**", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithNoWildcard() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"one", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void forSelfWithProfileNoWildcard() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"one:two", Collections.<String, String>emptyMap())),
				is(true));
	}

	@Test
	public void notForSelf() {
		assertThat(
				matcher.isForSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
								"one:two:9999", Collections.<String, String>emptyMap())),
				is(false));
	}

	@Test
	public void notFromSelf() {
		assertThat(
				matcher.isFromSelf(
						new EnvironmentChangeRemoteApplicationEvent(this, "one:two:9999",
								"foo:bar:spam", Collections.<String, String>emptyMap())),
				is(false));
	}

}
