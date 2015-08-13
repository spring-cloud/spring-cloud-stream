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
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * A (possibly temporary) alternative to {@link JarLauncher} that provides a public
 * {@link #launch(String[])} method.
 *
 * @author Mark Fisher
 * @author Marius Bogoevici
 */
public class ModuleJarLauncher extends ExecutableArchiveLauncher {

	private static final AsciiBytes LIB = new AsciiBytes("lib/");

	public ModuleJarLauncher(Archive archive) {
		super(archive);
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
		// System classloader is the -cp/-jar argument to java
		// Includes extra stuff that we may not want our modules to see
		ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
		// We want only the JRE classes as the parent
		// (the module uber-jar already contains everything it needs to run)
		ClassLoader parentToUse = systemClassLoader != null ? systemClassLoader.getParent() : null;
		return new LaunchedURLClassLoader(urls, parentToUse);
	}

}
