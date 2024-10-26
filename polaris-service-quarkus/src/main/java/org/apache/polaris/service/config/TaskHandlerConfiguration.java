/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class TaskHandlerConfiguration {

  private final int poolSize;
  private final boolean fixedSize;
  private final String threadNamePattern;

  public TaskHandlerConfiguration(
      @ConfigProperty(name = "polaris.tasks.pool-size") int poolSize,
      @ConfigProperty(name = "polaris.tasks.fixed-size") boolean fixedSize,
      @ConfigProperty(name = "polaris.tasks.thread-name-pattern") String threadNamePattern) {
    this.poolSize = poolSize;
    this.fixedSize = fixedSize;
    this.threadNamePattern = threadNamePattern;
  }

  public ExecutorService executorService() {
    return fixedSize
        ? Executors.newFixedThreadPool(poolSize, threadFactory())
        : Executors.newCachedThreadPool(threadFactory());
  }

  private ThreadFactory threadFactory() {
    return new ThreadFactoryBuilder().setNameFormat(threadNamePattern).setDaemon(true).build();
  }
}
