package io.vertx.sqlclient.impl.tracing;

import io.vertx.sqlclient.Tuple;

import java.util.List;

/**
 * A traceable query.
 */
public class QueryRequest {

  final TracingManager mgr;
  final String sql;
  final List<Tuple> tuples;

  public QueryRequest(TracingManager mgr, String sql, List<Tuple> tuples) {
    this.mgr = mgr;
    this.sql = sql;
    this.tuples = tuples;
  }

  public String sql() {
    return sql;
  }

  public List<Tuple> tuples() {
    return tuples;
  }
}
