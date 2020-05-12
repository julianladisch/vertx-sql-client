package io.vertx.pgclient;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.VertxTracerFactory;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.core.tracing.TracingOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class TracingTest extends PgTestBase {

  Vertx vertx;
  VertxTracer tracer;

  @Before
  public void setup() throws Exception {
    super.setup();
    vertx = Vertx.vertx(new VertxOptions().setTracingOptions(
      new TracingOptions().setEnabled(true).setFactory(tracingOptions -> new VertxTracer() {
        @Override
        public Object sendRequest(Context context, Object request, String operation, BiConsumer headers, TagExtractor tagExtractor) {
          return tracer.sendRequest(context, request, operation, headers, tagExtractor);
        }

        @Override
        public void receiveResponse(Context context, Object response, Object payload, Throwable failure, TagExtractor tagExtractor) {
          tracer.receiveResponse(context, response, payload, failure, tagExtractor);
        }
      }))
    );
  }

  @After
  public void teardown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testTracingOk(TestContext ctx) {
    AtomicBoolean called = new AtomicBoolean();
    String sql = "SELECT * FROM Fortune WHERE id=$1";
    Object expectedPayload = new Object();
    tracer = new VertxTracer<Object, Object>() {
      @Override
      public <R> Object sendRequest(Context context, R request, String operation, BiConsumer<String, String> headers, TagExtractor<R> tagExtractor) {
        Map<String, String> tags = tagExtractor.extract(request);
        ctx.assertEquals("client", tags.get("span.kind"));
        ctx.assertEquals("sql", tags.get("db.type"));
        ctx.assertEquals(sql, tags.get("db.statement"));
        return expectedPayload;
      }
      @Override
      public <R> void receiveResponse(Context context, R response, Object payload, Throwable failure, TagExtractor<R> tagExtractor) {
        ctx.assertEquals(payload, expectedPayload);
        called.set(true);
      }
    };
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn
        .preparedQuery(sql)
        .execute(Tuple.of(1), ctx.asyncAssertSuccess(res -> {
          ctx.assertTrue(called.get());
          conn.close();
        }));
    }));
  }

  @Test
  public void testTracingFailure(TestContext ctx) {
    AtomicBoolean called = new AtomicBoolean();
    tracer = new VertxTracer<Object, Object>() {
      @Override
      public <R> Object sendRequest(Context context, R request, String operation, BiConsumer<String, String> headers, TagExtractor<R> tagExtractor) {
        return null;
      }
      @Override
      public <R> void receiveResponse(Context context, R response, Object payload, Throwable failure, TagExtractor<R> tagExtractor) {
        ctx.assertNull(response);
        ctx.assertNotNull(failure);
        called.set(true);
      }
    };
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn
        .preparedQuery("SELECT 1 / $1")
        .execute(Tuple.of(0), ctx.asyncAssertFailure(err -> {
          ctx.assertTrue(called.get());
          conn.close();
        }));
    }));
  }

  @Test
  public void testMappingFailure(TestContext ctx) {
    RuntimeException failure = new RuntimeException();
    AtomicInteger called = new AtomicInteger();
    String sql = "SELECT * FROM Fortune WHERE id=$1";
    tracer = new VertxTracer<Object, Object>() {
      @Override
      public <R> Object sendRequest(Context context, R request, String operation, BiConsumer<String, String> headers, TagExtractor<R> tagExtractor) {
        return null;
      }
      @Override
      public <R> void receiveResponse(Context context, R response, Object payload, Throwable failure, TagExtractor<R> tagExtractor) {
        ctx.assertEquals(1, called.incrementAndGet());
      }
    };
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn
        .preparedQuery(sql)
        .mapping(row -> {
          throw failure;
        })
        .execute(Tuple.of(1), ctx.asyncAssertFailure(err -> {
          ctx.assertEquals(1, called.get());
          conn.close();
        }));
    }));
  }
}
