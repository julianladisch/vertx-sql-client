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

package io.vertx.sqlclient.impl.command;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.QueryResultHandler;
import io.vertx.sqlclient.impl.RowDesc;

import java.util.function.Function;
import java.util.stream.Collector;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */

public abstract class QueryCommandBase<T> extends CommandBase<Boolean> {

  public static final Collector<Row, Void, Void> NULL_COLLECTOR = Collector.of(() -> null, (v,row) -> {}, (v1, v2) -> null, Function.identity());

  protected final QueryResultHandler<T> resultHandler;
  protected final Collector<Row, ?, T> collector;
  protected final boolean autoCommit;

  QueryCommandBase(boolean autoCommit, Collector<Row, ?, T> collector, QueryResultHandler<T> resultHandler) {
    this.autoCommit = autoCommit;
    this.resultHandler = resultHandler;
    this.collector = collector;
  }

  protected QueryCommandBase(QueryCommandBase<T> that, QueryResultHandler<T> resultHandler) {
    this.autoCommit = that.autoCommit;
    this.collector = that.collector;
    this.resultHandler = new QueryResultHandler<T>() {
      @Override
      public <V> void addProperty(PropertyKind<V> property, V value) {
        resultHandler.addProperty(property, value);
        that.resultHandler.addProperty(property, value);
      }
      @Override
      public void handleResult(int updatedCount, int size, RowDesc desc, T result, Throwable failure) {
        resultHandler.handleResult(updatedCount, size, desc, result, failure);
        that.resultHandler.handleResult(updatedCount, size, desc, result, failure);
      }
    };
  }

  public QueryResultHandler<T> resultHandler() {
    return resultHandler;
  }

  public boolean isAutoCommit() {
    return autoCommit;
  }

  public Collector<Row, ?, T> collector() {
    return collector;
  }

  public abstract String sql();

}
