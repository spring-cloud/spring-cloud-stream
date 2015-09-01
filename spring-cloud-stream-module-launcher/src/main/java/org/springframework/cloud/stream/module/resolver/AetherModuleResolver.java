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

package org.springframework.cloud.stream.module.resolver;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.JavaScopes;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * An implementation of ModuleResolver using <a href="http://www.eclipse.org/aether/>aether</a> to resolve the module
 * artifact (uber jar) in a local Maven repository, downloading the latest update from a remote repository if
 * necessary.
 *
 * @author David Turanski
 * @author Mark Fisher
 * @author Marius Bogoevici
 */
public class AetherModuleResolver implements ModuleResolver {

	private static final Log log = LogFactory.getLog(AetherModuleResolver.class);

	private static final String DEFAULT_CONTENT_TYPE = "default";

	private final File localRepository;

	private final List<RemoteRepository> remoteRepositories;

	private final RepositorySystem repositorySystem;

	/**
	 * Create an instance specifying the locations of the local and remote repositories.
	 * @param localRepository the root path of the local maven repository
	 * @param remoteRepositories a Map containing pairs of (repository ID,repository URL). This
	 * may be null or empty if the local repository is off line.
	 */
	public AetherModuleResolver(File localRepository, Map<String, String> remoteRepositories) {
		Assert.notNull(localRepository, "Local repository path cannot be null");
		if (log.isDebugEnabled()) {
			log.debug("Local repository: " + localRepository);
			if (!CollectionUtils.isEmpty(remoteRepositories)) {
				// just listing the values, ids are simply informative
				log.debug("Remote repositories: " + StringUtils.collectionToCommaDelimitedString(remoteRepositories.values()));
			}
		}
		if (!localRepository.exists()) {
			Assert.isTrue(localRepository.mkdirs(),
					"Unable to create directory for local repository: " + localRepository);
		}
		this.localRepository = localRepository;
		this.remoteRepositories = new LinkedList<>();
		if (!CollectionUtils.isEmpty(remoteRepositories)) {
			for (Map.Entry<String, String> remoteRepo : remoteRepositories.entrySet()) {
				RemoteRepository remoteRepository = new RemoteRepository.Builder(remoteRepo.getKey(),
						DEFAULT_CONTENT_TYPE, remoteRepo.getValue()).build();
				this.remoteRepositories.add(remoteRepository);
			}
		}
		repositorySystem = newRepositorySystem();
	}

	/**
	 * Resolve an artifact and return its location in the local repository. Aether performs the normal
	 * Maven resolution process ensuring that the latest update is cached to the local repository.
	 * @param root the Maven coordinates of the artifact
	 * @return a {@ link FileSystemResource} representing the resolved artifact in the local repository
	 * @throws a RuntimeException if the artifact does not exist or the resolution fails
	 */
	@Override
	public Resource resolve(Coordinates root) {
		return this.resolve(root, null, null)[0];
	}

	/*
	 * Create a session to manage remote and local synchronization.
	 */
	private DefaultRepositorySystemSession newRepositorySystemSession(RepositorySystem system, String localRepoPath) {
		DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
		LocalRepository localRepo = new LocalRepository(localRepoPath);
		session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
		return session;
	}

	/*
	 * Aether's components implement {@link org.eclipse.aether.spi.locator.Service} to ease manual wiring.
	 * Using the prepopulated {@link DefaultServiceLocator}, we need to register the repository connector 
	 * and transporter factories
	 */
	private RepositorySystem newRepositorySystem() {
		DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
		locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
		locator.addService(TransporterFactory.class, FileTransporterFactory.class);
		locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
		locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
			@Override
			public void serviceCreationFailed(Class<?> type, Class<?> impl, Throwable exception) {
				throw new RuntimeException(exception);
			}
		});
		return locator.getService(RepositorySystem.class);
	}

	@Override
	public Resource[] resolve(Coordinates root, Coordinates[] includes, CoordinatesFilter[] excludes) {
		Assert.hasText(root.getGroupId(), "'groupId' cannot be blank.");
		Assert.hasText(root.getArtifactId(), "'artifactId' cannot be blank.");
		Assert.hasText(root.getExtension(), "'extension' cannot be blank.");
		Assert.hasText(root.getVersion(), "'version' cannot be blank.");

		List<Resource> result = new ArrayList<>();

		Artifact rootArtifact = toArtifact(root);

		RepositorySystemSession session = newRepositorySystemSession(repositorySystem,
				localRepository.getAbsolutePath());

		if (ObjectUtils.isEmpty(includes) && ObjectUtils.isEmpty(excludes)) {
			ArtifactResult resolvedArtifact;
			try {
				resolvedArtifact = repositorySystem.resolveArtifact(session,
						new ArtifactRequest(rootArtifact, remoteRepositories, JavaScopes.RUNTIME));
			}
			catch (ArtifactResolutionException e) {
				throw new RuntimeException(e);
			}
			result.add(toResource(resolvedArtifact));
		}
		else {
			try {
				CollectRequest collectRequest = new CollectRequest();
				collectRequest.setRoot(new Dependency(rootArtifact, JavaScopes.RUNTIME));
				for (Coordinates include : includes) {
					collectRequest.addDependency(new Dependency(toArtifact(include), JavaScopes.RUNTIME));
				}
				DependencyResult dependencyResult = repositorySystem.resolveDependencies(session, new DependencyRequest(collectRequest, null));
				for (ArtifactResult artifactResult : dependencyResult.getArtifactResults()) {
					// we are only interested in the jars or zips
					if ("jar".equalsIgnoreCase(artifactResult.getArtifact().getExtension())) {
						result.add(toResource(artifactResult));
					}
				}
			} catch (DependencyResolutionException e) {
				throw new RuntimeException(e);
			}
		}
		return result.toArray(new Resource[result.size()]);
	}

	public FileSystemResource toResource(ArtifactResult resolvedArtifact) {
		return new FileSystemResource(resolvedArtifact.getArtifact().getFile());
	}

	private Artifact toArtifact(Coordinates root) {
		return new DefaultArtifact(root.getGroupId(),
				root.getArtifactId(),
				root.getClassifier() != null ? root.getClassifier() : "",
				root.getExtension(),
				root.getVersion());
	}
}
