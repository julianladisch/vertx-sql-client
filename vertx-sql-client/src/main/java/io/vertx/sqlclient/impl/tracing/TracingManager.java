package io.vertx.sqlclient.impl.tracing;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.spi.tracing.TagExtractor;
import io.vertx.core.spi.tracing.VertxTracer;
import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.QueryResultHandler;
import io.vertx.sqlclient.impl.RowDesc;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.impl.command.ExtendedBatchQueryCommand;
import io.vertx.sqlclient.impl.command.ExtendedQueryCommand;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import io.vertx.sqlclient.impl.command.SimpleQueryCommand;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Tracing manager.
 */
public class TracingManager {

  enum RequestTags {

    // Generic
    PEER_ADDRESS("peer.address", q -> q.mgr.address),
    PEER_SERVICE("peer.service", q -> "todo"),
    SPAN_KIND("span.kind", q -> "client"),

    // DB
    // See https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/database.md

    DB_USER("db.user", q -> q.mgr.user),
    DB_INSTANCE("db.instance", q -> q.mgr.database),
    DB_URL("db.url", q -> "todo"),
    DB_STATEMENT("db.statement", QueryRequest::sql),
    DB_TYPE("db.type", q -> "sql");

    final String name;
    final Function<QueryRequest, String> fn;

    RequestTags(String name, Function<QueryRequest, String> fn) {
      this.name = name;
      this.fn = fn;
    }
  }

  private static final TagExtractor<QueryRequest> REQUEST_TAG_EXTRACTOR = new TagExtractor<QueryRequest>() {

    private final RequestTags[] TAGS = RequestTags.values();

    @Override
    public int len(QueryRequest obj) {
      return TAGS.length;
    }
    @Override
    public String name(QueryRequest obj, int index) {
      return TAGS[index].name;
    }
    @Override
    public String value(QueryRequest obj, int index) {
      return TAGS[index].fn.apply(obj);
    }
  };

  private final String address;
  private final String host;
  private final int port;
  private final String user;
  private final String database;
  private final VertxTracer tracer;

  public TracingManager(VertxTracer tracer, SqlConnectOptions options) {
    this.tracer = tracer;
    this.address = options.getHost() + ":" + options.getPort();
    this.host = options.getHost();
    this.port = options.getPort();
    this.user = options.getUser();
    this.database = options.getDatabase();
  }

  public <R> CommandBase<R> wrap(ContextInternal context, CommandBase<R> cmd) {
    if (cmd instanceof QueryCommandBase<?>) {
      cmd = wrap(context, (QueryCommandBase) cmd);
    }
    return cmd;
  }

  private <T> QueryCommandBase<T> wrap(ContextInternal context, QueryCommandBase<T> cmd) {
    List<Tuple> tuples;
    if (cmd instanceof ExtendedQueryCommand) {
      tuples = Collections.singletonList(((ExtendedQueryCommand)cmd).params());
    } else if (cmd instanceof ExtendedBatchQueryCommand) {
      tuples = ((ExtendedBatchQueryCommand)cmd).params();
    } else {
      tuples = Collections.emptyList();
    }
    QueryRequest request = new QueryRequest(this, cmd.sql(), tuples);
    Object payload = tracer.sendRequest(context, request, "Query", (k,v) -> {}, REQUEST_TAG_EXTRACTOR);
    QueryResultHandler<T> prevResultHandler = cmd.resultHandler();
    QueryResultHandler<T> nextResultHandler = new QueryResultHandler<T>() {
      @Override
      public <V> void addProperty(PropertyKind<V> property, V value) {
        prevResultHandler.addProperty(property, value);
      }
      @Override
      public void handleResult(int updatedCount, int size, RowDesc desc, T result, Throwable failure) {
        tracer.receiveResponse(context, result, payload, failure, TagExtractor.empty());
        prevResultHandler.handleResult(updatedCount, size, desc, result, failure);
      }
    };
    Handler<AsyncResult<Boolean>> handler = cmd.handler;
    if (cmd instanceof SimpleQueryCommand<?>) {
      SimpleQueryCommand<T> queryCmd = (SimpleQueryCommand<T>) cmd;
      cmd = new SimpleQueryCommand<>(queryCmd.sql(), queryCmd.isSingleton(), queryCmd.isAutoCommit(), queryCmd.collector(), nextResultHandler);
    } else if (cmd instanceof ExtendedQueryCommand<?>) {
      ExtendedQueryCommand<T> queryCmd = (ExtendedQueryCommand<T>) cmd;
      cmd = new ExtendedQueryCommand<>(queryCmd.preparedStatement(), queryCmd.params(), queryCmd.fetch(), queryCmd.cursorId(), queryCmd.isSuspended(), queryCmd.isAutoCommit(), queryCmd.collector(), nextResultHandler);
    } else {
      ExtendedBatchQueryCommand<T> queryCmd = (ExtendedBatchQueryCommand<T>) cmd;
      cmd = new ExtendedBatchQueryCommand<>(queryCmd.preparedStatement(), queryCmd.params(), queryCmd.fetch(), queryCmd.cursorId(), queryCmd.isSuspended(), queryCmd.isAutoCommit(), queryCmd.collector(), nextResultHandler);
    }
    cmd.handler = ar -> {
      if (ar.failed()) {
        tracer.receiveResponse(context, null, payload, ar.cause(), TagExtractor.empty());
      }
      handler.handle(ar);
    };
    return cmd;
  }
}
