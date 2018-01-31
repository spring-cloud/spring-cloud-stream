/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus;

import java.util.Comparator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.PathMatcher;
import org.springframework.util.StringUtils;

import static org.springframework.util.StringUtils.tokenizeToStringArray;

/**
 * {@link BusPathMatcher} that matches application context ids with multiple, comma-separated, profiles.
 * Original https://gist.github.com/kelapure/61d3f948acf478cc95225ff1d7d239c4
 *
 * See https://github.com/spring-cloud/spring-cloud-config/issues/678
 *
 * @author Rohit Kelapure
 * @author Spencer Gibb
 */
public class DefaultBusPathMatcher implements PathMatcher {

	private static final Log log = LogFactory.getLog(DefaultBusPathMatcher.class);

	private final PathMatcher delagateMatcher;

	public DefaultBusPathMatcher(PathMatcher delagateMatcher) {
		this.delagateMatcher = delagateMatcher;
	}

	protected boolean matchMultiProfile(String pattern, String idToMatch) {

		log.debug("matchMultiProfile : " + pattern + ", " + idToMatch);

		// parse the id
		String[] tokens = tokenizeToStringArray(idToMatch,":");
		if (tokens.length <= 1) {
			// no parts, default to delegate which already returned false;
			return false;
		}
		String selfProfiles = tokens[1];

		// short circuit if possible
		String[] profiles = tokenizeToStringArray(selfProfiles,",");

		if (profiles.length == 1) {
			// there aren't multiple profiles to check, the delegate match was
			// originally false so return what delegate determined
			return false;
		}

		// gather candidate ids with a single profile rather than a comma separated list
		String[] idsWithSingleProfile = new String[profiles.length];

		for (int i = 0; i < profiles.length; i++) {
			//replace comma separated profiles with single profile
			String profile = profiles[i];
			String[] newTokens = new String[tokens.length];
			System.arraycopy(tokens, 0, newTokens, 0, tokens.length);
			newTokens[1] = profile;
			idsWithSingleProfile[i] = StringUtils.arrayToDelimitedString(newTokens, ":");
		}

		for (String id : idsWithSingleProfile) {
			if (delagateMatcher.match(pattern, id)) {
				log.debug("matched true");
				return true;
			}
		}

		log.debug("matched false");
		return false;
	}

	@Override
	public boolean isPattern(String path) {
		return delagateMatcher.isPattern(path);
	}

	@Override
	public boolean match(String pattern, String path) {
		log.debug("In match: " + pattern + ", " + path);
		if (!delagateMatcher.match(pattern, path)) {
			return matchMultiProfile(pattern, path);
		}
		return true;
	}

	@Override
	public boolean matchStart(String pattern, String path) {
		return delagateMatcher.matchStart(pattern, path);
	}

	@Override
	public String extractPathWithinPattern(String pattern, String path) {
		return delagateMatcher.extractPathWithinPattern(pattern, path);
	}

	@Override
	public Map<String, String> extractUriTemplateVariables(String pattern, String path) {
		return delagateMatcher.extractUriTemplateVariables(pattern, path);
	}

	@Override
	public Comparator<String> getPatternComparator(String path) {
		return delagateMatcher.getPatternComparator(path);
	}

	@Override
	public String combine(String pattern1, String pattern2) {
		return delagateMatcher.combine(pattern1, pattern2);
	}
}
