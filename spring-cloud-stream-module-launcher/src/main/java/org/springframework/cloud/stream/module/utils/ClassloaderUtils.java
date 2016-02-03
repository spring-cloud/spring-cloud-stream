/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.module.utils;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.boot.loader.LaunchedURLClassLoader;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class ClassloaderUtils {

	private static final Log log = LogFactory.getLog(ClassloaderUtils.class);

	/**
	 * Creates a ClassLoader for the launched modules by merging the URLs supplied as argument with the URLs that
	 * make up the additional classpath of the launched JVM (retrieved from the application classloader), and
	 * setting the extension classloader of the JVM as parent, if accessible.
	 *
	 * @param urls a list of library URLs
	 * @return the resulting classloader
	 */
	public static ClassLoader createModuleClassloader(URL[] urls) {
		ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
		if (log.isDebugEnabled()) {
			log.debug("systemClassLoader is " + systemClassLoader);
		}
		if (systemClassLoader instanceof URLClassLoader) {
			// add the URLs of the application classloader to the created classloader
			// to compensate for LaunchedURLClassLoader not delegating to parent to retrieve resources
			@SuppressWarnings("resource")
			URLClassLoader systemUrlClassLoader = (URLClassLoader) systemClassLoader;
			List<URL> systemURLs = new ArrayList<>();
			for (URL url: systemUrlClassLoader.getURLs()) {
				// this will help avoiding any tests using the classes directory in the system class loader.
				if (!url.getFile().endsWith("classes/")) {
					systemURLs.add(url);
				}
			}
			URL[] filteredSystemURLs = systemURLs.toArray(new URL[0]);
			URL[] mergedUrls = new URL[urls.length + filteredSystemURLs.length];
			if (log.isDebugEnabled()) {
				log.debug("Original URLs: " +
						StringUtils.arrayToCommaDelimitedString(urls));
				log.debug("Java Classpath URLs: " +
						StringUtils.arrayToCommaDelimitedString(filteredSystemURLs));
			}
			System.arraycopy(urls, 0, mergedUrls, 0, urls.length);
			System.arraycopy(filteredSystemURLs, 0, mergedUrls, urls.length, filteredSystemURLs.length);
			// add the extension classloader as parent to the created context, if accessible
			if (log.isDebugEnabled()) {
				log.debug("Classloader URLs: " + StringUtils.arrayToCommaDelimitedString(mergedUrls));
			}
			return new LaunchedURLClassLoader(mergedUrls, systemUrlClassLoader.getParent());
		}
		return new LaunchedURLClassLoader(urls, systemClassLoader);
	}
}
