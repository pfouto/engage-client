package site.ycsb.db;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public interface KeyspaceManager {
  void nextOp();

  void incorporateWriteMigrateResponse(ResultSet execute) throws UnknownHostException;

  void addBlockingDepsCustomPayload(BoundStatement boundStmt);

  ByteBuffer getMetadata();

  String currentHost();

  String currentKeyspace();

  boolean justMigrated();

  String getPrevHost();

  void incorporateReadResponse(Row row) throws IOException;

  void incorporateWriteResponse(ResultSet execute) throws IOException;

  enum SessionGuarantees {
    RYW(1), MR(2), WFR(3), MW(4), CAUSAL(5);

    private final int value;

    SessionGuarantees(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  enum Protocol {
    saturn, engage, edgegage, bayou, engage2;
  }

}
