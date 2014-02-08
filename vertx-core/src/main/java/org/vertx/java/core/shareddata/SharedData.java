/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package org.vertx.java.core.shareddata;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.core.shareddata.impl.DefaultSharedMap;
import org.vertx.java.core.shareddata.impl.DefaultSharedSet;
import org.vertx.java.core.spi.cluster.ClusterManager;

/**
 * Sometimes it is desirable to share immutable data between different event loops, for example to implement a
 * cache of data.<p>
 * This class allows instances of shared data structures to be looked up and used from different event loops.<p>
 * The data structures themselves will only allow certain data types to be stored into them. This shields you from
 * worrying about any thread safety issues might occur if mutable objects were shared between event loops.<p>
 * The following types can be stored in a shareddata data structure:<p>
 * <pre>
 *   {@link String}
 *   {@link Integer}
 *   {@link Long}
 *   {@link Double}
 *   {@link Float}
 *   {@link Short}
 *   {@link Byte}
 *   {@link Character}
 *   {@code byte[]} - this will be automatically copied, and the copy will be stored in the structure.
 *   {@link org.vertx.java.core.buffer.Buffer} - this will be automatically copied, and the copy will be stored in the
 *   structure.
 * </pre>
 * <p>
 *
 * Instances of this class are thread-safe.<p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class SharedData {

  private static final Logger log = LoggerFactory.getLogger(SharedData.class);

  private ConcurrentMap<Object, ConcurrentSharedMap<?, ?>> maps = new ConcurrentHashMap<>();
  private ConcurrentMap<Object, SharedSet<?>> sets = new ConcurrentHashMap<>();

  private ClusterManager manager;

  public SharedData() {

  }

  public SharedData(ClusterManager manager) {
    this.manager = manager;
  }

  /**
   * Return a {@code Map} with the specific {@code name}.
   * 
   * All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Map} instance.
   * 
   * If a map with this name does not exist, a new local shared map is instantiated and returned.
   */
  public <K, V> ConcurrentSharedMap<K, V> getMap(String name) {
    return getMap(name, false);
  }

  /**
   * Return a {@code Map} with the specific {@code name}. 
   * All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Map} instance. 
   * 
   * @throws Exception - throws an exception if a map already exists, but does not match the clustered parameter.
   * <p>
   */
  public <K, V> ConcurrentSharedMap<K, V> getMap(String name, boolean clustered) {
    ConcurrentSharedMap<K, V> map = (ConcurrentSharedMap<K, V>) maps.get(name);
    if (map == null) {
      map = createAndReturnMap(name, clustered);
    }

    return map;
  }

  /**
   * Return a {@code Set} with the specific {@code name}. All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Set} instance. <p>
   */
  public <E> Set<E> getSet(String name) {
    return getSet(name, false);
  }

  /**
   * Return a {@code Set} with the specific {@code name}. All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Set} instance. <p>
   */
  public <E> Set<E> getSet(String name, boolean clustered) {
    SharedSet<E> set = (SharedSet<E>) sets.get(name);
    if (set == null) {
      set = createAndReturnSet(name, clustered);
    }
    return set;
  }

  /**
   * Remove the {@code Map} with the specific {@code name}.
   */
  public boolean removeMap(Object name) {
    return maps.remove(name) != null;
  }

  /**
   * Remove the {@code Set} with the specific {@code name}.
   */
  public boolean removeSet(Object name) {
    return sets.remove(name) != null;
  }

  private <K, V> ConcurrentSharedMap<K, V> createAndReturnMap(String name, boolean clustered) {
    // clustered
    clustered = this.manager != null && clustered;

    ConcurrentSharedMap<K, V> map;

    if (clustered) {
      map = new DefaultSharedMap<K, V>(this.manager.<K, V> getAsyncMap(name));
    } else {
      map = new DefaultSharedMap<K, V>();
    }

    ConcurrentSharedMap prev = maps.putIfAbsent(name, map);

    if (prev != null) {
      map = prev;
    }

    return map;
  }

  private <E> SharedSet<E> createAndReturnSet(String name, boolean clustered) {
    // clustered
    clustered = this.manager != null && clustered;

    SharedSet<E> set;

    if (clustered) {
      set = new DefaultSharedSet<E>(this.manager.<E, Object> getAsyncMap(name));
    } else {
      set = new DefaultSharedSet<E>();
    }

    SharedSet prev = sets.putIfAbsent(name, set);

    if (prev != null) {
      set = prev;
    }

    return set;
  }
}
