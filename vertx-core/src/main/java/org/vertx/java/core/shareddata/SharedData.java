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

import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.core.shareddata.impl.DefaultSharedMap;
import org.vertx.java.core.shareddata.impl.DefaultSharedSet;
import org.vertx.java.core.spi.cluster.ClusterManager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

  private ConcurrentMap<Object, ConcurrentSharedMap<?, ?>> instanceMaps = new ConcurrentHashMap<>();
  private ConcurrentMap<Object, ConcurrentSharedMap<?, ?>> clusterMaps = new ConcurrentHashMap<>();

  private ConcurrentMap<Object, SharedSet<?>> instanceSets = new ConcurrentHashMap<>();
  private ConcurrentMap<Object, SharedSet<?>> clusterSets = new ConcurrentHashMap<>();

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
    ConcurrentSharedMap<K, V> map;
    map = new DefaultSharedMap<K, V>();

    ConcurrentSharedMap prev = this.instanceMaps.putIfAbsent(name, map);

    if (prev != null) {
      map = prev;
    }

    return map;
  }

  /**
   * Return a cluster {@code Map} with the specific {@code name}.
   * All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Map} instance.
   * <p>
   */
  public <K, V> ConcurrentSharedMap<K, V> getClusterMap(String name) {
    ConcurrentSharedMap<K, V> map;

    if (this.manager != null) {
      map = new DefaultSharedMap<K, V>(this.manager.<K, V>getAsyncMap(name));
    } else {
      map = new DefaultSharedMap<K, V>();
    }

    ConcurrentSharedMap prev = this.clusterMaps.putIfAbsent(name, map);

    if (prev != null) {
      map = prev;
      //TODO: if previous map exists (i.e. a map was created in the time we checked then we need to destroy the one we just created)
    }

    return map;
  }

  /**
   * Return a {@code Set} with the specific {@code name}. All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Set} instance. <p>
   */
  public <E> Set<E> getSet(String name) {
    SharedSet<E> set;
    set = new DefaultSharedSet<E>();

    SharedSet prev = this.instanceSets.putIfAbsent(name, set);

    if (prev != null) {
      set = prev;
    }

    return set;
  }

  /**
   * Return a cluster {@code Set} with the specific {@code name}. All invocations of this method with the same value of {@code name}
   * are guaranteed to return the same {@code Set} instance. <p>
   */
  public <E> Set<E> getClusterSet(String name) {
    SharedSet<E> set;

    if (this.manager != null) {
      set = new DefaultSharedSet<E>(this.manager.<E, Object>getAsyncMap(name));
    } else {
      set = new DefaultSharedSet<E>();
    }

    SharedSet prev = this.clusterSets.putIfAbsent(name, set);

    if (prev != null) {
      set = prev;
      //TODO: if previous set exists (i.e. a map was created in the time we checked then we need to destroy the one we just created)
    }

    return set;
  }

  /**
   * Remove the {@code Map} with the specific {@code name}.
   */
  public boolean removeMap(Object name) {
    return instanceMaps.remove(name) != null;
  }

  /**
   * Remove the cluster {@code Map} with the specific {@code name}.
   */
  public boolean removeClusterMap(Object name) {
    return clusterMaps.remove(name) != null;
  }

  /**
   * Remove the {@code Set} with the specific {@code name}.
   */
  public boolean removeSet(Object name) {
    return instanceSets.remove(name) != null;
  }

  /**
   * Remove the cluster {@code Set} with the specific {@code name}.
   */
  public boolean removeClusterSet(Object name) {
    return clusterSets.remove(name) != null;
  }
}
