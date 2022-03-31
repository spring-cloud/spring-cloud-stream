/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.test.junit.rabbit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

/**
 * JUnit {@link org.junit.Rule} that detects the fact that RabbitMQ is available on
 * localhost.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Eric Bottard
 */
public class RabbitTestSupport
		extends AbstractExternalResourceTestSupport<CachingConnectionFactory> {

	private final boolean management;

	public RabbitTestSupport() {
		this(false);
	}

	public RabbitTestSupport(boolean management) {
		super("RABBIT");
		this.management = management;
	}

	@Override
	protected void obtainResource() throws Exception {
		resource = new CachingConnectionFactory("localhost");
		resource.createConnection().close();
		if (management) {
			Socket socket = SocketFactory.getDefault().createSocket("localhost", 15672);
			socket.close();
		}
	}

	@Override
	protected void cleanupResource() throws Exception {
		resource.destroy();
	}

	/**
	 * Test class to allow testing deferred entity declarations when RabbitMQ is down.
	 */
	public static class RabbitProxy {

		private static final Log LOGGER = LogFactory.getLog(RabbitProxy.class);

		private final int port;

		private final ExecutorService serverExec = Executors.newSingleThreadExecutor();

		private final ExecutorService socketExec = Executors.newCachedThreadPool();

		private volatile ServerSocket serverSocket;

		public RabbitProxy() throws IOException {
			ServerSocket serverSocket = ServerSocketFactory.getDefault()
					.createServerSocket(0);
			this.port = serverSocket.getLocalPort();
			serverSocket.close();
		}

		public int getPort() {
			return this.port;
		}

		public void start() throws IOException {
			this.serverSocket = ServerSocketFactory.getDefault()
					.createServerSocket(this.port, 10);
			LOGGER.info("Proxy started");
			this.serverExec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						while (true) {
							final Socket socket = serverSocket.accept();
							LOGGER.info("Accepted Connection");
							socketExec.execute(new Runnable() {

								@Override
								public void run() {
									try {
										final Socket rabbitSocket = SocketFactory
												.getDefault()
												.createSocket("localhost", 5672);
										socketExec.execute(new Runnable() {

											@Override
											public void run() {
												LOGGER.info("Running: " + rabbitSocket.getLocalPort());
												try {
													InputStream is = rabbitSocket
															.getInputStream();
													OutputStream os = socket
															.getOutputStream();
													int c;
													while ((c = is.read()) >= 0) {
														os.write(c);
													}
												}
												catch (IOException e) {
													try {
														socket.close();
														rabbitSocket.close();
													}
													catch (IOException e1) {
													}
												}
											}
										});
										InputStream is = socket.getInputStream();
										OutputStream os = rabbitSocket.getOutputStream();
										int c;
										while ((c = is.read()) >= 0) {
											os.write(c);
										}
									}
									catch (IOException e) {
										try {
											socket.close();
										}
										catch (IOException e1) {
										}
									}
								}

							});
						}
					}
					catch (IOException e) {
						try {
							serverSocket.close();
						}
						catch (IOException e1) {
						}
					}
				}
			});
		}

		public void stop() throws IOException {
			this.serverSocket.close();
		}

	}

}
