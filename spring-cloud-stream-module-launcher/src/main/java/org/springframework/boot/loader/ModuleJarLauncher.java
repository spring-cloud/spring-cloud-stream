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

package org.springframework.boot.loader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.boot.loader.archive.Archive;
import org.springframework.boot.loader.util.AsciiBytes;

/**
 * A (possibly temporary) alternative to {@link JarLauncher} that provides a
 * public {@link #launch(String[])} method.
 *
 * @author Mark Fisher
 */
public class ModuleJarLauncher extends ExecutableArchiveLauncher {

	private static final AsciiBytes LIB = new AsciiBytes("lib/");

	private final InputArgumentsJavaAgentDetector javaAgentDetector;


	public ModuleJarLauncher(Archive archive) {
		super(archive);
		this.javaAgentDetector = new InputArgumentsJavaAgentDetector();
	}

	@Override
	protected boolean isNestedArchive(Archive.Entry entry) {
		return !entry.isDirectory() && entry.getName().startsWith(LIB);
	}

	@Override
	protected void postProcessClassPathArchives(List<Archive> archives) throws Exception {
		archives.add(0, getArchive());
	}


	@Override
	public void launch(String[] args) {
		super.launch(args);
	}

	@Override
	protected ClassLoader createClassLoader(URL[] urls) throws Exception {
		Set<URL> copy = new LinkedHashSet<URL>(urls.length);
		ClassLoader loader = getDefaultClassLoader();
		if (loader instanceof URLClassLoader) {
			for (URL url : ((URLClassLoader) loader).getURLs()) {
				if (addDefaultClassloaderUrl(urls, url)) {
					copy.add(url);
				}
			}
		}
		Collections.addAll(copy, urls);
		return new LaunchedURLClassLoader(copy.toArray(new URL[copy.size()]), null);
	}


	private boolean addDefaultClassloaderUrl(URL[] urls, URL url) {
		String jarUrl = "jar:" + url + "!/";
		for (URL nestedUrl : urls) {
			if (nestedUrl.equals(url) || nestedUrl.toString().equals(jarUrl)) {
				return false;
			}
		}
		return !this.javaAgentDetector.isJavaAgentJar(url);
	}

	private static ClassLoader getDefaultClassLoader() {
		ClassLoader classloader = null;
		try {
			classloader = Thread.currentThread().getContextClassLoader();
		}
		catch (Throwable ex) {
			// Cannot access thread context ClassLoader - falling back to system class
			// loader...
		}
		if (classloader == null) {
			// No thread context class loader -> use class loader of this class.
			classloader = ExecutableArchiveLauncher.class.getClassLoader();
		}
		return classloader;
	}
}
