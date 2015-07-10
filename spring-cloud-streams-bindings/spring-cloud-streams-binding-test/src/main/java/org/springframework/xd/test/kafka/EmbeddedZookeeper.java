/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.springframework.xd.test.kafka;

import java.io.File;
import java.net.InetSocketAddress;

import kafka.utils.TestUtils$;
import kafka.utils.Utils$;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * A port of kafka.zk.EmbeddedZookeeper, compatible with Zookeeper 3.4 API
 *
 * @author Marius Bogoevici
 */
public class EmbeddedZookeeper {

	private String connectString;

	private File snapshotDir = TestUtils$.MODULE$.tempDir();

	private File logDir = TestUtils$.MODULE$.tempDir();

	private int tickTime = 500;

	private final ZooKeeperServer zookeeper;

	private int port;

	private final NIOServerCnxnFactory factory;

	public EmbeddedZookeeper(String connectString) throws Exception {
		this.connectString = connectString;
		port = Integer.parseInt(connectString.split(":")[1]);
		zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime);
		factory = new NIOServerCnxnFactory();
		factory.configure(new InetSocketAddress("127.0.0.1", port), 100);
		factory.startup(zookeeper);
	}

	public String getConnectString() {
		return connectString;
	}

	public File getSnapshotDir() {
		return snapshotDir;
	}

	public File getLogDir() {
		return logDir;
	}

	public int getTickTime() {
		return tickTime;
	}

	public ZooKeeperServer getZookeeper() {
		return zookeeper;
	}

	public int getPort() {
		return port;
	}

	public void shutdown() {
		try {
			zookeeper.shutdown();
		}
		catch (Exception e) {
			// ignore exception
		}
		try {
			factory.shutdown();
		}
		catch (Exception e) {
			// ignore exception
		}
		try {
			Utils$.MODULE$.rm(logDir);
		}
		catch (Exception e) {
			// ignore exception
		}
		try {
			Utils$.MODULE$.rm(snapshotDir);
		}
		catch (Exception e) {
			// ignore exception
		}
	}
}