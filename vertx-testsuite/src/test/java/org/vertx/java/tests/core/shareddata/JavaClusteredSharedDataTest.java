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

package org.vertx.java.tests.core.shareddata;

import org.junit.Test;
import org.vertx.java.testframework.TestBase;

import vertx.tests.core.shareddata.ClusteredSharedDataTest;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class JavaClusteredSharedDataTest extends TestBase {
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    startApp();
  }

  protected void startApp() throws Exception {
    super.startApp(ClusteredSharedDataTest.class.getName());
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testMap() throws Exception {
    startTest(getMethodName());
  }

  @Test
  public void testMapClustered() throws Exception {
    startTest(getMethodName());
  }

  @Test
  public void testMapTypes() throws Exception {
    startTest(getMethodName());
  }

  @Test
  public void testSetTypes() throws Exception {
    startTest(getMethodName());
  }

  @Test
  public void testSet() throws Exception {
    startTest(getMethodName());
  }
  
  @Test
  public void testSetClustered() throws Exception {
    startTest(getMethodName());
  }
}
