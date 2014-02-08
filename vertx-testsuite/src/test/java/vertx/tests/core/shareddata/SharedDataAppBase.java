package vertx.tests.core.shareddata;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.core.shareddata.ConcurrentSharedMap;
import org.vertx.java.core.shareddata.SharedData;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.fakecluster.FakeClusterManager;
import org.vertx.java.testframework.TestClientBase;
import org.vertx.java.testframework.TestUtils;

public abstract class SharedDataAppBase extends TestClientBase {
  private static final Logger log = LoggerFactory.getLogger(SharedDataAppBase.class);

  protected SharedData sharedData;

  @Override
  public void start(final Future<Void> startedResult) {
    super.start();

    if (isLocal()) {
      this.sharedData = vertx.sharedData();
    } else {
      try {
        VertxInternal vertxi = ((VertxInternal) vertx);
        ClusterManager clusterManager = new FakeClusterManager(vertxi);

        this.sharedData = new SharedData(clusterManager);
      } catch (Throwable e) {
        startedResult.setFailure(e);
      }
    }

    tu.appReady();
    startedResult.setResult(null);
  }

  @Override
  public void stop() {
    if (isLocal()) {
      super.stop();
    } else {
      // TODO: might need to do some cleanup of the cluster shared data space?
      super.stop();
    }

  }

  protected abstract boolean isLocal();

  public void testMap() throws Exception {
    /* Test Map Retrieval */
    final ConcurrentSharedMap<String, String> map = sharedData.getMap("foo");
    ConcurrentSharedMap<String, String> map2 = sharedData.getMap("foo");
    tu.azzert(map == map2);
    ConcurrentSharedMap<String, String> map3 = sharedData.getMap("bar");
    tu.azzert(map3 != map2);
    tu.azzert(sharedData.removeMap("foo"));
    ConcurrentSharedMap<String, String> map4 = sharedData.getMap("foo");
    tu.azzert(map4 != map3);

    /* Test Map Sync Operations */
    final String key = "key";

    map.put(key, "Hello");
    tu.azzert(map.get(key).equals("Hello"));

    map.put(key, "World");
    tu.azzert(map.get(key).equals("World"));

    map.remove(key);
    tu.azzert(map.get(key) == null);

    /* Test Map Async Operations */
    final CountDownLatch latch = new CountDownLatch(3);

    map.put(key, "Hello", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> event) {
        map.get(key, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            tu.azzert(event.result().equals("Hello"));
            latch.countDown();
          }
        });
      }
    });

    map.put(key, "World", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> event) {
        map.get(key, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            tu.azzert(event.result().equals("World"));
            latch.countDown();
          }
        });
      }
    });

    map.remove(key, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> event) {
        map.get(key, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            tu.azzert(event.result() == null);
            latch.countDown();
          }
        });
      }
    });

    latch.await(2, TimeUnit.SECONDS);
    tu.testComplete();
  }

  public void testMapClustered() throws Exception {
    final ConcurrentSharedMap<String, String> map = sharedData.getMap("foo", true);
    ConcurrentSharedMap<String, String> map2 = sharedData.getMap("foo", true);
    tu.azzert(map == map2);
    ConcurrentSharedMap<String, String> map3 = sharedData.getMap("bar", true);
    tu.azzert(map3 != map2);
    tu.azzert(sharedData.removeMap("foo"));
    ConcurrentSharedMap<String, String> map4 = sharedData.getMap("foo", true);
    tu.azzert(map4 != map3);

    /* Test Map Sync Operations */
    final String key = "key";

    map.put(key, "Hello");
    tu.azzert(map.get(key).equals("Hello"));

    map.put(key, "World");
    tu.azzert(map.get(key).equals("World"));

    map.remove(key);
    tu.azzert(map.get(key) == null);

    /* Test Map Async Operations */
    final CountDownLatch latch = new CountDownLatch(3);

    map.put(key, "Hello", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> event) {
        map.get(key, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            tu.azzert(event.result().equals("Hello"));
            latch.countDown();
          }
        });
      }
    });

    map.put(key, "World", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> event) {
        map.get(key, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            tu.azzert(event.result().equals("World"));
            latch.countDown();
          }
        });
      }
    });

    map.remove(key, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> event) {
        map.get(key, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> event) {
            tu.azzert(event.result() == null);
            latch.countDown();
          }
        });
      }
    });

    latch.await(2, TimeUnit.SECONDS);
    tu.testComplete();
  }

  public void testMapTypes() throws Exception {
    Map map = sharedData.getMap("foo");

    String key = "key";

    double d = new Random().nextDouble();
    map.put(key, d);
    tu.azzert(map.get(key).equals(d));

    float f = new Random().nextFloat();
    map.put(key, f);
    tu.azzert(map.get(key).equals(f));

    byte b = (byte) new Random().nextInt();
    map.put(key, b);
    tu.azzert(map.get(key).equals(b));

    short s = (short) new Random().nextInt();
    map.put(key, s);
    tu.azzert(map.get(key).equals(s));

    int i = new Random().nextInt();
    map.put(key, i);
    tu.azzert(map.get(key).equals(i));

    long l = new Random().nextLong();
    map.put(key, l);
    tu.azzert(map.get(key).equals(l));

    map.put(key, true);
    tu.azzert((Boolean) map.get(key));

    map.put(key, false);
    tu.azzert(!((Boolean) map.get(key)));

    char c = (char) new Random().nextLong();
    map.put(key, c);
    tu.azzert(map.get(key).equals(c));

    Buffer buff = TestUtils.generateRandomBuffer(100);
    map.put(key, buff);
    Buffer got1 = (Buffer) map.get(key);
    tu.azzert(got1 != buff); // Make sure it's copied
    tu.azzert(map.get(key).equals(buff));
    Buffer got2 = (Buffer) map.get(key);
    tu.azzert(got1 != got2); // Should be copied each time
    tu.azzert(got2 != buff);
    tu.azzert(map.get(key).equals(buff));

    byte[] bytes = TestUtils.generateRandomByteArray(100);
    map.put(key, bytes);
    byte[] bgot1 = (byte[]) map.get(key);
    tu.azzert(bgot1 != bytes);
    tu.azzert(TestUtils.byteArraysEqual(bytes, bgot1));
    byte[] bgot2 = (byte[]) map.get(key);
    tu.azzert(bgot2 != bytes);
    tu.azzert(bgot1 != bgot2);
    tu.azzert(TestUtils.byteArraysEqual(bytes, bgot2));

    try {
      map.put(key, new SomeOtherClass());
      tu.exception(new Exception("Should throw exception"), "Should throw exception");
    } catch (IllegalArgumentException e) {
      // OK
      tu.testComplete();
    }
  }

  public void testSetTypes() throws Exception {
    Set set = sharedData.getSet("foo");

    double d = new Random().nextDouble();
    set.add(d);
    tu.azzert(set.iterator().next().equals(d));
    set.clear();

    float f = new Random().nextFloat();
    set.add(f);
    tu.azzert(set.iterator().next().equals(f));
    set.clear();

    byte b = (byte) new Random().nextInt();
    set.add(b);
    tu.azzert(set.iterator().next().equals(b));
    set.clear();

    short s = (short) new Random().nextInt();
    set.add(s);
    tu.azzert(set.iterator().next().equals(s));
    set.clear();

    int i = new Random().nextInt();
    set.add(i);
    tu.azzert(set.iterator().next().equals(i));
    set.clear();

    long l = new Random().nextLong();
    set.add(l);
    tu.azzert(set.iterator().next().equals(l));
    set.clear();

    set.add(true);
    tu.azzert((Boolean) set.iterator().next());
    set.clear();

    set.add(false);
    tu.azzert(!((Boolean) set.iterator().next()));
    set.clear();

    char c = (char) new Random().nextLong();
    set.add(c);
    tu.azzert(set.iterator().next().equals(c));
    set.clear();

    Buffer buff = TestUtils.generateRandomBuffer(100);
    set.add(buff);
    Buffer got1 = (Buffer) set.iterator().next();
    tu.azzert(got1 != buff); // Make sure it's copied
    tu.azzert(set.iterator().next().equals(buff));
    Buffer got2 = (Buffer) set.iterator().next();
    tu.azzert(got1 != got2); // Should be copied on each get
    tu.azzert(got2 != buff);
    tu.azzert(set.iterator().next().equals(buff));
    set.clear();

    byte[] bytes = TestUtils.generateRandomByteArray(100);
    set.add(bytes);
    byte[] bgot1 = (byte[]) set.iterator().next();
    tu.azzert(bgot1 != bytes);
    tu.azzert(TestUtils.byteArraysEqual(bytes, bgot1));
    byte[] bgot2 = (byte[]) set.iterator().next();
    tu.azzert(bgot2 != bytes);
    tu.azzert(bgot1 != bgot2);
    tu.azzert(TestUtils.byteArraysEqual(bytes, bgot2));
    set.clear();

    try {
      set.add(new SomeOtherClass());
      tu.exception(new Exception("Should throw exception"), "Should throw exception");
    } catch (IllegalArgumentException e) {
      // OK
      tu.testComplete();
    }
  }

  public void testSet() throws Exception {
    Set<String> set = sharedData.getSet("foo");
    Set<String> set2 = sharedData.getSet("foo");
    assert (set == set2);
    Set<String> set3 = sharedData.getSet("bar");
    assert (set3 != set2);
    assert (sharedData.removeSet("foo"));
    Set<String> set4 = sharedData.getSet("foo");
    assert (set4 != set3);

    tu.testComplete();
  }

  public void testSetClustered() throws Exception {
    Set<String> set = sharedData.getSet("foo", true);
    Set<String> set2 = sharedData.getSet("foo", true);
    assert (set == set2);
    Set<String> set3 = sharedData.getSet("bar", true);
    assert (set3 != set2);
    assert (sharedData.removeSet("foo"));
    Set<String> set4 = sharedData.getSet("foo", true);
    assert (set4 != set3);

    tu.testComplete();
  }

  class SomeOtherClass {
  }

}
