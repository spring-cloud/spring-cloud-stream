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

package org.springframework.cloud.stream.module.launcher;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.springframework.boot.loader.ModuleJarLauncher;
import org.springframework.boot.loader.archive.JarFileArchive;

/**
 * Bootstrap for launching one or more modules. The module path(s), relative to the module home, must be provided via
 * the "modules" system property or "MODULES" environment variable as a comma-delimited list. The module home directory
 * itself may be provided via the "module.home" system property or "MODULE_HOME" environment variable. The default
 * module home directory is: /opt/spring/modules
 * <p>
 * To pass args to a module, prefix with the module name and a dot. The arg name will be de-qualified and passed along.
 * For example: <code>--foo.bar=123</code> becomes <code>--bar=123</code> and is only passed to the 'foo' module.
 *
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class ModuleLauncher {

	private static final String DEFAULT_MODULE_HOME = "/opt/spring/modules";

	private final File moduleHome;

	public ModuleLauncher(File moduleHome) {
		this.moduleHome = moduleHome;
	}

	public void launch(String[] modules, String[] args) {
		Executor executor = Executors.newFixedThreadPool(modules.length);
		for (String module : modules) {
			List<String> moduleArgs = new ArrayList<>(); 
			for (String arg : args) {
				if (arg.startsWith("--" + module + ".")) {
					moduleArgs.add("--" + arg.substring(module.length() + 3));
				}
			}
			moduleArgs.add("--spring.jmx.default-domain=" + module.replace("/", "."));
			executor.execute(new ModuleLaunchTask(moduleHome, module + ".jar", moduleArgs.toArray(new String[moduleArgs.size()])));
		}
	}

	public static void main(String[] args) throws Exception {
		String modules = System.getProperty("modules");
		if (modules == null) {
			modules = System.getenv("MODULES");
		}
		if (modules == null) {
			System.err.println("Either the 'modules' system property or 'MODULES' environment variable is required.");
			System.exit(1);
		}
		String moduleHome = System.getProperty("module.home");
		if (moduleHome == null) {
			moduleHome = System.getenv("MODULE_HOME");
		}
		if (moduleHome == null) {
			moduleHome = DEFAULT_MODULE_HOME;
		}
		ModuleLauncher launcher = new ModuleLauncher(new File(moduleHome));
		launcher.launch(modules.split(","), args);
	}


	private static class ModuleLaunchTask implements Runnable {

		private final File directory;

		private final String module;

		private final String[] args;

		ModuleLaunchTask(File directory, String module, String[] args) {
			this.directory = directory;
			this.module = module;
			this.args = args;
		}

		@Override
		public void run() {
			try {
				JarFileArchive jarFileArchive = new JarFileArchive(new File(directory, module));
				ModuleJarLauncher jarLauncher = new ModuleJarLauncher(jarFileArchive);
				jarLauncher.launch(args);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
