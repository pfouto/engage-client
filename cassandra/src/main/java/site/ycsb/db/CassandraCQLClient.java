/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package site.ycsb.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/**
 * Cassandra 2.x CQL client.
 * <p>
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb2";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";
  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String MAX_CONNECTIONS_PROPERTY = "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY = "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "cassandra.readtimeoutmillis";
  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";
  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);
  private static final ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> scanStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> scanAllStmt = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> deleteStmt = new AtomicReference<>();
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static Cluster cluster = null;
  private static Session session = null;
  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = writeConsistencyLevel.name();
  private static boolean trace = false;
  private static SessionGuarantees guarantees;
  private final Map<Inet4Address, Integer> readClock = new HashMap<>();
  private final Map<Inet4Address, Integer> writeClock = new HashMap<>();
  public static String keyspace;

  private ByteBuffer clockToBuffer(Map<Inet4Address, Integer> clock) {
    ByteBuffer allocate = ByteBuffer.allocate((clock.size() * 2 + 1) * 4);
    allocate.putInt(clock.size());
    clock.forEach((k, v) -> {
      allocate.put(k.getAddress());
      allocate.putInt(v);
    });
    return allocate;
  }

  private Map<Inet4Address, Integer> mergedClocks(Map<Inet4Address, Integer> rc, Map<Inet4Address, Integer> wc) {
    Map<Inet4Address, Integer> merged = new HashMap<>(rc);
    wc.forEach((k, v) -> merged.merge(k, v, Math::max));
    return merged;
  }

  private void addCustomPayload(BoundStatement boundStmt) {
    Map<String, ByteBuffer> customPayload = new HashMap<>();
    ByteBuffer guaranteesBuffer = ByteBuffer.allocate(4).putInt(guarantees.getValue()).flip();
    customPayload.put("g", guaranteesBuffer);
    if (guarantees == SessionGuarantees.WFR || guarantees == SessionGuarantees.MR)
      customPayload.put("c", clockToBuffer(readClock));
    else
      customPayload.put("c", clockToBuffer(writeClock));
    boundStmt.setOutgoingPayload(customPayload);
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (cluster != null) {
        return;
      }

      try {

        trace = Boolean.parseBoolean(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
            KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        boolean useSSL = Boolean.parseBoolean(getProperties().getProperty(USE_SSL_CONNECTION,
            DEFAULT_USE_SSL_CONNECTION));

        if ((username != null) && !username.isEmpty()) {
          Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
              .withPort(Integer.parseInt(port)).addContactPoints(hosts);
          if (useSSL) {
            clusterBuilder = clusterBuilder.withSSL();
          }
          cluster = clusterBuilder.build();
        } else {
          cluster = Cluster.builder().withPort(Integer.parseInt(port))
              .addContactPoints(hosts).build();
        }

        String maxConnections = getProperties().getProperty(MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setMaxConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(maxConnections));
        }

        String coreConnections = getProperties().getProperty(CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setCoreConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(coreConnections));
        }

        String connectTimoutMillis = getProperties().getProperty(CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(Integer.parseInt(connectTimoutMillis));
        }

        String readTimoutMillis = getProperties().getProperty(READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.parseInt(readTimoutMillis));
        }

        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {}\n", metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
          logger.info("Datacenter: {}; Host: {}; Rack: {}\n",
              discoveredHost.getDatacenter(), discoveredHost.getAddress(), discoveredHost.getRack());
        }
        session = cluster.connect();

      } catch (Exception e) {
        throw new DBException(e);
      }

      //ENGAGE
      guarantees = SessionGuarantees.valueOf(getProperties().getProperty("engage.session_guarantees"));
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        readStmts.clear();
        scanStmts.clear();
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.set(null);
        scanAllStmt.set(null);
        deleteStmt.set(null);
        session.close();
        cluster.close();
        cluster = null;
        session = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get() : readStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        stmt = session.prepare(selectBuilder.from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
            .limit(1));
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
            readAllStmt.getAndSet(stmt) :
            readStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQueryString());
      logger.debug("key = {}", key);
      BoundStatement boundStmt = stmt.bind(key);

      addCustomPayload(boundStmt);
      ResultSet rs = session.execute(boundStmt);

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = row.getColumnDefinitions();

      for (ColumnDefinitions.Definition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName(), null);
        }
      }

      return Status.OK;

    } catch (Exception e) {
      System.out.println("ERROR: " + e);
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * <p>
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      PreparedStatement stmt = (fields == null) ? scanAllStmt.get() : scanStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        Select selectStmt = selectBuilder.from(table);

        // The statement builder is not setup right for tokens.
        // So, we need to build it manually.
        String initialStmt = selectStmt.toString();
        StringBuilder scanStmt = new StringBuilder();
        scanStmt.append(initialStmt, 0, initialStmt.length() - 1);
        scanStmt.append(" WHERE ");
        scanStmt.append(QueryBuilder.token(YCSB_KEY));
        scanStmt.append(" >= ");
        scanStmt.append("token(");
        scanStmt.append(QueryBuilder.bindMarker());
        scanStmt.append(")");
        scanStmt.append(" LIMIT ");
        scanStmt.append(QueryBuilder.bindMarker());

        stmt = session.prepare(scanStmt.toString());
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
            scanAllStmt.getAndSet(stmt) :
            scanStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQueryString());
      logger.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      ResultSet rs = session.execute(stmt.bind(startkey, recordcount));

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName(), null);
          }
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      logger.error(
          MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = updateStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Update updateStmt = QueryBuilder.update(keyspace, table);

        // Add fields
        for (String field : fields) {
          updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
        }
        updateStmt.with(QueryBuilder.set("clock", QueryBuilder.bindMarker()));
        // Add key
        updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        stmt = session.prepare(updateStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQueryString());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      BoundStatement boundStmt = stmt.bind();
      for (int i = 0; i < vars.size() - 2; i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }
      boundStmt.setBytes(vars.size() - 2, clockToBuffer(mergedClocks(readClock, writeClock)).flip());
      // Add key
      boundStmt.setString(vars.size() - 1, key);

      addCustomPayload(boundStmt);
      System.out.println("Update: " + key + " " + readClock + " " + writeClock);
      ResultSet execute = session.execute(boundStmt);
      ByteBuffer c = execute.getExecutionInfo().getIncomingPayload().get("c");
      byte[] addrBytes = new byte[4];
      c.get(addrBytes);
      InetAddress addr = Inet4Address.getByAddress(addrBytes);
      int val = c.getInt();
      writeClock.merge((Inet4Address) addr, val, Math::max);
      System.out.println(writeClock);


      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = insertStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Insert insertStmt = QueryBuilder.insertInto(table);

        // Add key
        insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());

        // Add fields
        for (String field : fields) {
          insertStmt.value(field, QueryBuilder.bindMarker());
        }
        insertStmt.value("clock", QueryBuilder.bindMarker());

        stmt = session.prepare(insertStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQueryString());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add key
      BoundStatement boundStmt = stmt.bind().setString(0, key);

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      for (int i = 1; i < vars.size() - 1; i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }
      boundStmt.setBytes(vars.size() - 1, clockToBuffer(mergedClocks(readClock, writeClock)).flip());
      addCustomPayload(boundStmt);
      System.out.println("Insert: " + key);
      ResultSet execute = session.execute(boundStmt);
      ByteBuffer c = execute.getExecutionInfo().getIncomingPayload().get("c");
      byte[] addrBytes = new byte[4];
      c.get(addrBytes);
      InetAddress addr = Inet4Address.getByAddress(addrBytes);
      int val = c.getInt();
      writeClock.merge((Inet4Address) addr, val, Math::max);
      System.out.println(writeClock);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      PreparedStatement stmt = deleteStmt.get();

      // Prepare statement on demand
      if (stmt == null) {
        stmt = session.prepare(QueryBuilder.delete().from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())));
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = deleteStmt.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQueryString());
      logger.debug("key = {}", key);

      session.execute(stmt.bind(key));

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }


  //ENGAGE PARAMETERS
  private enum SessionGuarantees {
    RYW(1), MR(2), WFR(3), MW(4);

    private final int value;

    SessionGuarantees(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

}
