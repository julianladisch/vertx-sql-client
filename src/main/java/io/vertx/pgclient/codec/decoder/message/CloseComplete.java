package io.vertx.pgclient.codec.decoder.message;

import io.vertx.pgclient.codec.Message;

/**
 * @author <a href="mailto:emad.albloushi@gmail.com">Emad Alblueshi</a>
 */

public class CloseComplete implements Message {

  public static final CloseComplete INSTANCE = new CloseComplete();

  private CloseComplete() {}

  @Override
  public String toString() {
    return "CloseComplete{}";
  }
}
