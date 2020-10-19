/*
 * Copyright (C) 2017 Julien Viet
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
 *
 */

package io.vertx.sqlclient.impl.pool;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.ConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(VertxUnitRunner.class)
public class ConnectionPoolTest {

  @Test
  public void testSimple() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder = new SimpleHolder();
    pool.acquire(holder);
    assertEquals(1, queue.size());
    assertFalse(holder.isComplete());
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    assertTrue(holder.isConnected());
    assertNotNull(conn.holder);
    assertNotSame(conn, holder.connection());
    holder.init();
    holder.close();
  }

  @Test
  public void testRecycle() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder1.init();
    SimpleHolder holder2 = new SimpleHolder();
    pool.acquire(holder2);
    assertFalse(holder2.isComplete());
    assertEquals(0, queue.size());
    holder1.close();
    assertEquals(0, conn.closed);
    assertEquals(0, holder1.closed());
    assertTrue(holder2.isConnected());
    assertEquals(0, queue.size());
  }

  @Test
  public void testConnectionCreation() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    SimpleHolder holder2 = new SimpleHolder();
    pool.acquire(holder2);
    assertEquals(1, queue.size()); // Check that we won't create more connection than max size
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    assertTrue(holder1.isConnected());
    assertEquals(0, queue.size());
  }

  @Test
  public void testConnClose() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder1.init();
    SimpleHolder holder2 = new SimpleHolder();
    pool.acquire(holder2);
    assertFalse(holder2.isComplete());
    assertEquals(0, queue.size());
    conn.close();
    assertEquals(1, holder1.closed());
    assertEquals(1, queue.size());
    assertFalse(holder2.isComplete());
  }

  @Test
  public void testConnectionCloseInPool() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder = new SimpleHolder();
    pool.acquire(holder);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder.init();
    holder.close();
    conn.close();
    assertEquals(0, pool.available());
  }

  @Test
  public void testDoubleConnectionClose() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder = new SimpleHolder();
    pool.acquire(holder);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder.init();
    conn.close();
    try {
      conn.close();
      fail();
    } catch (IllegalStateException ignore) {
    }
  }

  @Test
  public void testDoubleConnectionRelease() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder = new SimpleHolder();
    pool.acquire(holder);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder.init();
    Future<Void> fut = holder.close();
    assertTrue(fut.succeeded());
    fut = holder.close();
    assertTrue(fut.failed());
  }

  @Test
  public void testDoubleConnectionAcquire() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    SimpleHolder holder = new SimpleHolder();
    pool.acquire(holder);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder.init();
    try {
      holder.init();
      fail();
    } catch (IllegalStateException ignore) {
    }
  }

  @Test
  public void testReleaseConnectionWhenWaiterQueueIsEmpty() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 2);
    // Acquire a connection from the pool for holder1
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    SimpleConnection conn1 = new SimpleConnection();
    queue.connect(conn1);
    holder1.init();
    // Acquire a connection from the pool for holder2
    SimpleHolder holder2 = new SimpleHolder();
    pool.acquire(holder2);
    // Release the first connection so the second waiter gets the connection
    holder1.close();
    // The connection should be put back in the pool
    assertEquals(1, pool.available());
    // Satisfy the holder with connection it actually asked for
    SimpleConnection conn2 = new SimpleConnection();
    queue.connect(conn2);
    holder2.init();
  }

  @Test
  public void testReleaseClosedConnectionShouldNotAddBackTheConnectionToThePool() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1);
    // Acquire a connection from the pool for holder1
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    SimpleConnection conn1 = new SimpleConnection();
    queue.connect(conn1);
    holder1.init();
    // Close connection
    conn1.close();
    holder1.close();
    assertEquals(pool.available(), 0);
  }

  @Test
  public void testMaxQueueSize1() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1, 0);
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    holder1.init();
    SimpleHolder holder2 = new SimpleHolder();
    pool.acquire(holder2);
    assertTrue(holder2.isFailed());
  }

  @Test
  public void testMaxQueueSize2() {
    SimpleHolder holder2 = new SimpleHolder();
    SimpleConnection conn = new SimpleConnection();
    ConnectionPool[] poolRef = new ConnectionPool[1];
    ConnectionPool pool = new ConnectionPool(new ConnectionFactory() {
      @Override
      public Future<Connection> connect() {
        Promise<Connection> promise = Promise.promise();
        poolRef[0].acquire(holder2);
        assertFalse(holder2.isComplete());
        promise.complete(conn);
        assertFalse(holder2.isComplete());
        return promise.future();
      }
    }, 1, 0);
    poolRef[0] = pool;
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    assertTrue(holder1.isComplete());
    assertTrue(holder2.isFailed());
  }

  @Test
  public void testConnectionFailure() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 1, 0);
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    Exception cause = new Exception();
    queue.fail(cause);
    assertTrue(holder1.isFailed());
    assertSame(cause, holder1.failure());
    assertEquals(0, pool.available());
    assertEquals(0, pool.size());
    SimpleHolder holder2 = new SimpleHolder();
    pool.acquire(holder2);
    SimpleConnection conn = new SimpleConnection();
    queue.connect(conn);
    assertTrue(holder2.isConnected());
    assertEquals(0, pool.available());
    assertEquals(1, pool.size());
  }

  @Test
  public void testAcquireOnlyConnectOnce() {
    ConnectionQueue queue = new ConnectionQueue();
    ConnectionPool pool = new ConnectionPool(queue, 10, 0);
    SimpleHolder holder1 = new SimpleHolder();
    pool.acquire(holder1);
    assertEquals(1, queue.size());
  }

  @Test(expected = NullPointerException.class)
  public void testConnectionReleaseDelayWithoutContext() {
    new ConnectionPool(new ConnectionQueue(), null, new PoolOptions().setConnectionReleaseDelay(60000));
  }

  @Test
  public void testExpire(TestContext ctx) {
    Async async = ctx.async();
    Vertx vertx = Vertx.vertx();
    Context context = Vertx.vertx().getOrCreateContext();
    context.runOnContext(run -> ctx.verify(verify -> {
      ConnectionQueue queue = new ConnectionQueue();
      PoolOptions poolOptions = new PoolOptions()
          .setConnectionReleaseDelay(1);
      ConnectionPool pool = new ConnectionPool(queue, context, poolOptions);
      SimpleHolder holder = new SimpleHolder();
      pool.acquire(holder);
      SimpleConnection conn = new SimpleConnection();
      queue.connect(conn);
      holder.init();
      holder.close();
      ctx.assertEquals(1, pool.available());
      ctx.assertEquals(1, pool.allSize());
      ctx.assertEquals(0, conn.closed);
      // waiting longer than connectionReleaseDelay results in expire()
      vertx.setTimer(5, t -> {
        ctx.assertEquals(0, pool.available());
        ctx.assertEquals(0, pool.allSize());
        ctx.assertEquals(1, conn.closed);
        async.complete();
      });
    }));
  }

  @Test
  public void testRecycleNoIdleTimer(TestContext ctx) {
    Async async = ctx.async();
    Vertx vertx = Vertx.vertx();
    Context context = Vertx.vertx().getOrCreateContext();
    context.runOnContext(run -> ctx.verify(verify -> {
      ConnectionQueue queue = new ConnectionQueue();
      PoolOptions poolOptions = new PoolOptions()
          .setMaxSize(1)
          .setConnectionReleaseDelay(5);
      ConnectionPool pool = new ConnectionPool(queue, context, poolOptions);
      SimpleHolder holder1 = new SimpleHolder();
      pool.acquire(holder1);
      SimpleConnection conn = new SimpleConnection();
      queue.connect(conn);
      holder1.init();

      SimpleHolder holder2 = new SimpleHolder();
      pool.acquire(holder2);
      holder1.close();  // holder2 acquires conn, no idle timer

      vertx.setTimer(10, t2 -> {
        ctx.assertEquals(0, pool.available());
        ctx.assertEquals(1, pool.allSize());
        ctx.assertEquals(0, conn.closed);
        async.complete();
      });
    }));
  }

  @Test
  public void testCancelIdleTimer(TestContext ctx) {
    Async async = ctx.async();
    Vertx vertx = Vertx.vertx();
    Context context = Vertx.vertx().getOrCreateContext();
    context.runOnContext(run -> ctx.verify(verify -> {
      ConnectionQueue queue = new ConnectionQueue();
      PoolOptions poolOptions = new PoolOptions()
          .setConnectionReleaseDelay(5);
      ConnectionPool pool = new ConnectionPool(queue, context, poolOptions);
      SimpleHolder holder1 = new SimpleHolder();
      pool.acquire(holder1);
      SimpleConnection conn = new SimpleConnection();
      queue.connect(conn);
      holder1.init();
      holder1.close();
      SimpleHolder holder2 = new SimpleHolder();
      pool.acquire(holder2);  // this cancels the idle timer
      vertx.setTimer(10, t2 -> {
        ctx.assertEquals(0, pool.available());
        ctx.assertEquals(1, pool.allSize());
        ctx.assertEquals(0, conn.closed);
        async.complete();
      });
    }));
  }
}
