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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.boot.loader.ModuleJarLauncher;
import org.springframework.boot.loader.archive.JarFileArchive;
import org.springframework.cloud.stream.module.resolver.ModuleResolver;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A component that launches one or more modules, delegating their resolution to an
 * underlying {@link ModuleResolver}.
 *
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @author Eric Bottard
 */
public class ModuleLauncher {

	private static final String DEFAULT_EXTENSION = "jar";

	private static final String DEFAULT_CLASSIFIER = "exec";

	private static final Pattern COORDINATES_PATTERN =
			Pattern.compile("([^: ]+):([^: ]+)(:([^: ]*)(:([^: ]+))?)?:([^: ]+)");

	private final ModuleResolver moduleResolver;

	/**
	 * Creates a module launcher using the provided module resolver
	 * @param moduleResolver the module resolver instance to use
	 */
	public ModuleLauncher(ModuleResolver moduleResolver) {
		this.moduleResolver = moduleResolver;
	}

	/**
	 * Launches one or more modules, with the corresponding arguments, if any.
	 *
	 * Modules must be passed in "natural" left to right order.
	 *
	 * The format of each module must conform to the <a href="http://www.eclipse.org/aether">Aether</a> convention:
	 * <code>&lt;groupId&gt;:&lt;artifactId&gt;[:&lt;extension&gt;[:&lt;classifier&gt;]]:&lt;version&gt;</code>
	 *
	 * @param moduleLaunchRequests a list of modules with their arguments
	 */
	public void launch(List<ModuleLaunchRequest> moduleLaunchRequests) {
		List<ModuleLaunchRequest> reversed = new ArrayList<>(moduleLaunchRequests);
		Collections.reverse(reversed);
		for (ModuleLaunchRequest moduleLaunchRequest : reversed) {
			String module = moduleLaunchRequest.getModule();
			moduleLaunchRequest.addArgument("spring.jmx.default-domain", module.replace("/", ".").replace(":", "."));
			launchModule(module, toArgArray(moduleLaunchRequest.getArguments()));
		}
	}

	/**
	 * Converts a set of semantic program arguments to "command line program arguments" that is, to the
	 * {@literal --foo=bar} form.
	 */
	private String[] toArgArray(Map<String, String> args) {
		String[] result = new String[args.size()];
		int i = 0;
		for (Map.Entry<String, String> kv : args.entrySet()) {
			result[i++] = String.format("--%s=%s", kv.getKey(), kv.getValue());
		}
		return result;
	}

	private void launchModule(String module, String[] args) {
		try {
			Resource resource = resolveModule(module);
			JarFileArchive jarFileArchive = new JarFileArchive(resource.getFile());
			ModuleJarLauncher jarLauncher = new ModuleJarLauncher(jarFileArchive);
			jarLauncher.launch(args);
		}
		catch (IOException e) {
			throw new RuntimeException("failed to launch module: " + module, e);
		}
	}

	private Resource resolveModule(String coordinates) {
		Matcher matcher = COORDINATES_PATTERN.matcher(coordinates);
		Assert.isTrue(matcher.matches(), "Bad artifact coordinates " + coordinates
				+ ", expected format is <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>");
		String groupId = matcher.group(1);
		String artifactId = matcher.group(2);
		String extension = StringUtils.hasLength(matcher.group(4)) ? matcher.group(4) : DEFAULT_EXTENSION;
		String classifier = StringUtils.hasLength(matcher.group(6)) ? matcher.group(6) : DEFAULT_CLASSIFIER;
		String version = matcher.group(7);
		return this.moduleResolver.resolve(groupId, artifactId, extension, classifier, version);
	}

}
