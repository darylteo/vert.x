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

package org.vertx.java.spi.cluster.impl.hazelcast;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.IMap;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.spi.Action;
import org.vertx.java.core.spi.VertxSPI;
import org.vertx.java.core.spi.cluster.AsyncMap;

class HazelcastAsyncMap<K, V> implements AsyncMap<K, V> {

  private final VertxSPI vertx;
  private final IMap<K, V> map;

  public HazelcastAsyncMap(VertxSPI vertx, IMap<K, V> map) {
    this.vertx = vertx;
    this.map = map;
  }

  @Override
  public void get(final Object o, Handler<AsyncResult<V>> asyncResultHandler) {
    vertx.executeBlocking(new Action<V>() {
      public V perform() {
        return map.get(o);
      }
    }, asyncResultHandler);
  }

  @Override
  public void put(final K k, final V v, Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(new Action<Void>() {
      public Void perform() {
        map.put(k, HazelcastServerID.convertServerID(v));
        return null;
      }
    }, completionHandler);
  }

  @Override
  public void remove(final Object o, Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(new Action<Void>() {
      public Void perform() {
        map.remove(o);
        return null;
      }
    }, completionHandler);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return map.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return map.remove(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return map.replace(key, oldValue, newValue);
  }

  @Override
  public V replace(K key, V value) {
    return map.replace(key, value);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    map.putAll(m);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    return map.entrySet();
  }
}
