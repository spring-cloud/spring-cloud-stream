/*
 * Copyright 2012-2019 the original author or authors.
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

package org.springframework.cloud.bus;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.util.AntPathMatcher;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dave Syer
 *
 */
public class PathServiceMatcherTests {

	private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

	private ServiceMatcher matcher;

	@Before
	public void init() {
		initMatcher("one:two:8888");
	}

	private void initMatcher(String id) {
		BusProperties properties = new BusProperties();
		properties.setId(id);
		DefaultBusPathMatcher pathMatcher = new DefaultBusPathMatcher(new AntPathMatcher(":"));
		this.matcher = new PathServiceMatcher(pathMatcher, properties.getId());
	}

	@Test
	public void fromSelf() {
		assertThat(this.matcher.isFromSelf(
				new EnvironmentChangeRemoteApplicationEvent(this, "one:two:8888", "foo:bar:spam", EMPTY_MAP))).isTrue();
	}

	@Test
	public void forSelf() {
		assertThat(this.matcher.isForSelf(
				new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "one:two:8888", EMPTY_MAP))).isTrue();
	}

	@Test
	public void forSelfWithWildcard() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "one:two:*", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void forSelfWithGlobalWildcard() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "**", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void forSelfWithWildcardName() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "o*", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void forSelfWithWildcardNameAndProfile() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "o*:t*", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void forSelfWithWildcardString() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "o*", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void notForSelfWithWildCardNameAndMismatchingProfile() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "o*:f*", EMPTY_MAP)))
						.isFalse();
	}

	@Test
	public void forSelfWithDoubleWildcard() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "one:**", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void forSelfWithNoWildcard() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "one", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void forSelfWithProfileNoWildcard() {
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "one:two", EMPTY_MAP)))
						.isTrue();
	}

	@Test
	public void notForSelf() {
		assertThat(this.matcher.isForSelf(
				new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "one:two:9999", EMPTY_MAP)))
						.isFalse();
	}

	@Test
	public void notFromSelf() {
		assertThat(this.matcher.isFromSelf(
				new EnvironmentChangeRemoteApplicationEvent(this, "one:two:9999", "foo:bar:spam", EMPTY_MAP)))
						.isFalse();
	}

	/**
	 * see gh-678
	 */
	@Test
	public void forSelfWithMultipleProfiles() {
		initMatcher("customerportal:dev,cloud:80");
		assertThat(this.matcher.isForSelf(
				new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "customerportal:cloud:*", EMPTY_MAP)))
						.isTrue();
	}

	/**
	 * see gh-678
	 */
	@Test
	public void notForSelfWithMultipleProfiles() {
		initMatcher("customerportal:dev,cloud:80");
		assertThat(this.matcher
				.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam", "bar:cloud:*", EMPTY_MAP)))
						.isFalse();
	}

	/**
	 * see gh-678
	 */
	@Test
	public void notForSelfWithMultipleProfilesDifferentPort() {
		initMatcher("customerportal:dev,cloud:80");
		assertThat(this.matcher.isForSelf(new EnvironmentChangeRemoteApplicationEvent(this, "foo:bar:spam",
				"customerportal:cloud:8008", EMPTY_MAP))).isFalse();
	}

}
