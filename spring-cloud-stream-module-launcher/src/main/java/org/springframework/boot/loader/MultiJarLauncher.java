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

import java.lang.reflect.Method;
import java.net.URL;
import java.util.List;

import org.springframework.boot.loader.archive.Archive;
import org.springframework.boot.loader.util.AsciiBytes;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * A {@link Launcher} that provides a public {@link #launch(String[])} method.
 *
 * @author Marius Bogoevici
 * @author Eric Bottard
 */
public class MultiJarLauncher extends Launcher {

	private final List<Archive> archives;

	/**
	 * A list of archives, the first of which is expected to be a Spring boot uberJar
	 * 
	 * @param archives
	 */
	public MultiJarLauncher(List<Archive> archives) {
		Assert.notEmpty(archives, "A list of archives must be provided");
		this.archives = archives;
	}

	@Override
	protected String getMainClass() throws Exception {
		return archives.get(0).getManifest().getMainAttributes().getValue("Start-Class");
	}

	@Override
	protected List<Archive> getClassPathArchives() throws Exception {
		return archives;
	}

	@Override
	// TODO: this method is protected in Spring Boot but we need it to be public here
	public void launch(String[] args) {
		super.launch(args);
	}

	@Override
	protected void launch(String[] args, String mainClass, ClassLoader classLoader)
			throws Exception {
		if (ClassUtils.isPresent(
				"org.apache.catalina.webresources.TomcatURLStreamHandlerFactory",
				classLoader)) {
			// Ensure the method is invoked on a class that is loaded by the provided
			// class loader (not the current context class loader):
			Method method = ReflectionUtils
					.findMethod(
							classLoader
							.loadClass("org.apache.catalina.webresources.TomcatURLStreamHandlerFactory"),
							"disable");
			ReflectionUtils.invokeMethod(method, null);
		}
		super.launch(args, mainClass, classLoader);
	}

	@Override
	protected ClassLoader createClassLoader(URL[] urls) throws Exception {
		ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
		// try to retrieve the extension classloader
		ClassLoader extensionClassLoader = systemClassLoader != null ? systemClassLoader.getParent() : null;
		// set the classloader for the module as the extension classloader if available
		// fall back to the system classloader (which can also be null) if not available
		ClassLoader classLoaderForModule = extensionClassLoader != null ? extensionClassLoader : systemClassLoader;
		return new LaunchedURLClassLoader(urls, classLoaderForModule);
	}

}
