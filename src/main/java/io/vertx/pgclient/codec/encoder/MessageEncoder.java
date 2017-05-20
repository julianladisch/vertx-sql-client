package io.vertx.pgclient.codec.encoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.vertx.pgclient.codec.Message;
import io.vertx.pgclient.codec.encoder.message.*;
import io.vertx.pgclient.codec.util.Util;

import static io.vertx.pgclient.codec.encoder.message.type.MessageType.*;
import static io.vertx.pgclient.codec.util.Util.*;
import static java.nio.charset.StandardCharsets.*;


/**
 *
 * Encoder for <a href="https://www.postgresql.org/docs/9.5/static/protocol.html">PostgreSQL protocol</a>
 *
 * @author <a href="mailto:emad.albloushi@gmail.com">Emad Alblueshi</a>
 */

public class MessageEncoder extends MessageToByteEncoder<Message> {

  private static final ByteBuf BUFF_USER = Unpooled.copiedBuffer("user", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_DATABASE = Unpooled.copiedBuffer("database", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_APPLICATION_NAME = Unpooled.copiedBuffer("application_name", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_VERTX_PG_CLIENT = Unpooled.copiedBuffer("vertx-pg-client", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_CLIENT_ENCODING = Unpooled.copiedBuffer("client_encoding", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_UTF8 = Unpooled.copiedBuffer("utf8", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_DATE_STYLE = Unpooled.copiedBuffer("DateStyle", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_ISO = Unpooled.copiedBuffer("ISO", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_EXTRA_FLOAT_DIGITS = Unpooled.copiedBuffer("extra_float_digits", UTF_8).asReadOnly();
  private static final ByteBuf BUFF_2 = Unpooled.copiedBuffer("2", UTF_8).asReadOnly();

  @Override
  protected void encode(ChannelHandlerContext ctx, Message message, ByteBuf out) throws Exception {
    if (message.getClass() == StartupMessage.class) {
      encodeStartupMessage(message, out);
    } else if (message.getClass() == PasswordMessage.class) {
      encodePasswordMessage(message, out);
    } else if(message.getClass() == Query.class) {
      encodeQuery(message, out);
    } else if(message.getClass() == Terminate.class) {
      encodeTerminate(out);
    } else if(message.getClass() == Parse.class) {
      encodeParse(message , out);
    } else if(message.getClass() == Bind.class) {
      encodeBind(message , out);
    } else if(message.getClass() == Describe.class) {
      encodeDescribe(message , out);
    } else if(message.getClass() == Execute.class) {
      encodeExecute(message , out);
    } else if(message.getClass() == Close.class) {
      encodeClose(message , out);
    } else if(message.getClass() == Sync.class) {
      encodeSync(out);
    }
  }

  private void encodeStartupMessage(Message message, ByteBuf out) {

    StartupMessage startup = (StartupMessage) message;

    out.writeInt(0);
    // protocol version
    out.writeShort(3);
    out.writeShort(0);

    writeCString(out, BUFF_USER);
    Util.writeCStringUTF8(out, startup.getUsername());
    writeCString(out, BUFF_DATABASE);
    Util.writeCStringUTF8(out, startup.getDatabase());
    writeCString(out, BUFF_APPLICATION_NAME);
    writeCString(out, BUFF_VERTX_PG_CLIENT);
    writeCString(out, BUFF_CLIENT_ENCODING);
    writeCString(out, BUFF_UTF8);
    writeCString(out, BUFF_DATE_STYLE);
    writeCString(out, BUFF_ISO);
    writeCString(out, BUFF_EXTRA_FLOAT_DIGITS);
    writeCString(out, BUFF_2);

    out.writeByte(0);
    out.setInt(0, out.writerIndex());
  }

  private void encodePasswordMessage(Message message, ByteBuf out) {
    PasswordMessage password = (PasswordMessage) message;
    out.writeByte(PASSWORD_MESSAGE);
    out.writeInt(0);
    Util.writeCStringUTF8(out, password.getHash());
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeQuery(Message message, ByteBuf out) {
    Query query = (Query) message;
    out.writeByte(QUERY);
    out.writeInt(0);
    Util.writeCStringUTF8(out, query.getQuery());
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeTerminate(ByteBuf out) {
    out.writeByte(TERMINATE);
    out.writeInt(4);
  }

  private void encodeParse(Message message, ByteBuf out) {
    Parse parse = (Parse) message;
    out.writeByte(PARSE);
    out.writeInt(0);
    Util.writeCStringUTF8(out, parse.getStatement() != null ? parse.getStatement() : "");
    Util.writeCStringUTF8(out, parse.getQuery());
    out.writeShort(0); // no parameter data types (OIDs)
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeBind(Message message, ByteBuf out) {
    Bind bind = (Bind) message;
    byte[][] paramValues = bind.getParamValues();
    out.writeByte(BIND);
    out.writeInt(0);
    Util.writeCStringUTF8(out, bind.getPortal() != null ? bind.getPortal() : "");
    Util.writeCStringUTF8(out, bind.getStatement() != null ? bind.getStatement() : "");
    out.writeShort(0);
    // Parameter values
    out.writeShort(paramValues.length);
    for (int c = 0; c < paramValues.length; ++c) {
      if (paramValues[c] == null) {
        // NULL value
        out.writeInt(-1);
      } else {
        // Not NULL value
        out.writeInt(paramValues[c].length);
        out.writeBytes(paramValues[c]);
      }
    }
    // TEXT format
    out.writeShort(0);
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeDescribe(Message message, ByteBuf out) {
    Describe describe = (Describe) message;
    out.writeByte(DESCRIBE);
    out.writeInt(0);
    out.writeByte('S'); // 'S' to describe a prepared statement or 'P' to describe a portal
    Util.writeCStringUTF8(out, describe.getStatement() != null ? describe.getStatement() : "");
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeExecute(Message message, ByteBuf out) {
    Execute execute = (Execute) message;
    out.writeByte(EXECUTE);
    out.writeInt(0);
    Util.writeCStringUTF8(out, execute.getStatement() != null ? execute.getStatement() : "");
    out.writeInt(execute.getRowCount()); // Zero denotes "no limit" maybe for ReadStream<Row>
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeClose(Message message, ByteBuf out) {
    Close close = (Close) message;
    out.writeByte(CLOSE);
    out.writeInt(0);
    out.writeByte('S'); // 'S' to close a prepared statement or 'P' to close a portal
    Util.writeCStringUTF8(out, close.getStatement() != null ? close.getStatement() : "");
    out.setInt(1, out.writerIndex() - 1);
  }

  private void encodeSync(ByteBuf out) {
    out.writeByte(SYNC);
    out.writeInt(4);
  }
}

