package org.vertx.java.core.shareddata;

import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

public interface SharedSet<E> extends Set<E> {
  /**
   * Adds an element to the set, asynchronously.
   * @param element The element
   * @param completionHandler - this will be called some time later to signify the value has been put
   */
  void add(E element, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Tests an element for membership in this set, asynchronously.
   * 
   * @param o element whose presence in this set is to be tested
   * @param resultHandler
   */
  void contains(Object o, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Removes an element from the set, asynchronously.
   * @param o Object to be removed 
   * @param completionHandler - this will be called some time later to signify the value has been removed
   */
  void remove(Object o, Handler<AsyncResult<Boolean>> resultHandler);
}
