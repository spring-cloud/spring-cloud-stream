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

package org.springframework.cloud.stream.module.utils;

import java.net.URL;
import java.net.URLClassLoader;

import org.springframework.boot.loader.LaunchedURLClassLoader;

/**
 * @author Marius Bogoevici
 */
public class ClassloaderUtils {

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
		if (systemClassLoader instanceof URLClassLoader) {
			// add the URLs of the application classloader to the created classloader
			// to compensate for LaunchedURLClassLoader not delegating to parent to retrieve resources
			@SuppressWarnings("resource")
			URLClassLoader systemUrlClassLoader = (URLClassLoader) systemClassLoader;
			URL[] mergedUrls = new URL[urls.length + systemUrlClassLoader.getURLs().length];
			System.arraycopy(urls, 0, mergedUrls, 0, urls.length);
			System.arraycopy(systemUrlClassLoader.getURLs(), 0, mergedUrls, urls.length,
					systemUrlClassLoader.getURLs().length);
			// add the extension classloader as parent to the created context, if accessible
			return new LaunchedURLClassLoader(mergedUrls, systemUrlClassLoader.getParent());
		}
		return new LaunchedURLClassLoader(urls, systemClassLoader);
	}
}
