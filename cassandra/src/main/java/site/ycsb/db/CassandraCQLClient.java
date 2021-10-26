package site.ycsb.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CassandraCQLClient extends DB {

  public static final String YCSB_KEY = "y_id";
  public static final String LOCAL_DC_PROPERTY = "localdc";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
  public static final String MAX_CONNECTIONS_PROPERTY = "cassandra.maxconnections";
  public static final String MAX_REQUESTS_PER_CONNECTION_PROPERTY = "cassandra.maxrequests";
  public static final String CORE_CONNECTIONS_PROPERTY = "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "cassandra.readtimeoutmillis";
  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  private static final Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);

  //                                  Host:Partition, Statement
  private static final ConcurrentMap<String, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, PreparedStatement> insertStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, PreparedStatement> updateStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, PreparedStatement> readAllStmt = new ConcurrentHashMap<>();

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static Map<String, Cluster> clusters = null;
  private static Map<String, Session> sessions = null;
  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = writeConsistencyLevel.name();
  private static boolean trace = false;
  private static String guarantees;
  private static String protocol;
  private static List<String> keyspacesLocal;
  private static Set<String> keyspacesRemote;
  private static Map<String, Set<String>> hostsKeyspaces;
  private static int nOpsLocal;
  private static int nOpsRemote;
  private static String localDC;
  private static boolean migrate;
  private static KsManager ksManagerType;

  //Per client thread
  private KeyspaceManager ksManager;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    try {
      // Keep track of number of calls to init (for later cleanup)
      INIT_COUNT.incrementAndGet();

      // Synchronized so that we only have a single
      // cluster/session instance for all the threads.
      synchronized (INIT_COUNT) {

        // Check if the cluster has already been initialized
        if (clusters == null) {


          clusters = new HashMap<>();
          sessions = new HashMap<>();

          // ----------------- ENGAGE
          guarantees = getProperties().getProperty("engage.session_guarantees");
          protocol = getProperties().getProperty("engage.protocol");
          nOpsLocal = Integer.parseInt(getProperties().getProperty("engage.ops_local"));
          nOpsRemote = Integer.parseInt(getProperties().getProperty("engage.ops_remote"));
          migrate = Boolean.parseBoolean(getProperties().getProperty("engage.migration_enabled"));
          ksManagerType = KsManager.valueOf(getProperties().getProperty("engage.ksmanager"));

          localDC = getProperties().getProperty(LOCAL_DC_PROPERTY);
          if (localDC == null) {
            throw new DBException(String.format(
                "Required property \"%s\" missing for CassandraCQLClient",
                LOCAL_DC_PROPERTY));
          }

          //Read tree
          keyspacesRemote = new HashSet<>();
          keyspacesLocal = new ArrayList<>();
          hostsKeyspaces = new HashMap<>();

          String treeFile = getProperties().getProperty("engage.tree_file");
          BufferedReader reader = Files.newBufferedReader(Paths.get(treeFile));
          Tree tree = new Gson().fromJson(reader, Tree.class);
          reader.close();

          keyspacesLocal.addAll(tree.getNodes().get(localDC).getPartitions());

          System.err.println("LocalDC: " + localDC + " " + keyspacesLocal.get(0));

          tree.getNodes().forEach((h, pi) -> {
            hostsKeyspaces.put(h, new HashSet<>(pi.getPartitions()));
            pi.getPartitions().forEach(ks -> {
              if (!keyspacesLocal.contains(ks)) keyspacesRemote.add(ks);
            });
          });
          //System.out.println("Local: " + keyspacesLocal);
          //System.out.println("Remote: " + keyspacesRemote);
          //System.out.println("All: " + hostsKeyspaces);

          trace = Boolean.parseBoolean(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

          String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

          readConsistencyLevel = ConsistencyLevel.valueOf(
              getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                  READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
          writeConsistencyLevel = ConsistencyLevel.valueOf(
              getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                  WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

          Set<String> hostsToConnect = null;
          if(ksManagerType == KsManager.regular) {
            hostsToConnect = hostsKeyspaces.keySet();
          } else if (ksManagerType == KsManager.visibility){
            hostsToConnect = Set.of(localDC);
          }
          for (String host : hostsToConnect) {
            Cluster cluster = Cluster.builder().withPort(Integer.parseInt(port))
                .addContactPoints(host).build();
            String maxConnections = getProperties().getProperty(MAX_CONNECTIONS_PROPERTY);
            if (maxConnections != null) {
              cluster.getConfiguration().getPoolingOptions()
                  .setMaxConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(maxConnections));
            }
            String maxRequests = getProperties().getProperty(MAX_REQUESTS_PER_CONNECTION_PROPERTY);
            if (maxRequests != null) {
              cluster.getConfiguration().getPoolingOptions()
                  .setMaxRequestsPerConnection(HostDistance.LOCAL, Integer.parseInt(maxRequests));
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
            //logger.info("Connected to cluster: {}\n", metadata.getClusterName());
            for (Host discoveredHost : metadata.getAllHosts()) {
              //logger.info("Datacenter: {}; Host: {}; Rack: {}\n",
                  //discoveredHost.getDatacenter(), discoveredHost.getEndPoint(), discoveredHost.getRack());
            }
            Session session = cluster.connect();

            clusters.put(host, cluster);
            sessions.put(host, session);
          }


        }
      } // synchronized
      if(ksManagerType == KsManager.regular) {
        ksManager = new KeyspaceManagerRegular(localDC, keyspacesLocal, keyspacesRemote,
            hostsKeyspaces, nOpsLocal, nOpsRemote, protocol, guarantees);
      } else if (ksManagerType == KsManager.visibility){
        ksManager = new KeyspaceManagerVisib(localDC, keyspacesLocal, protocol, guarantees);
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
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
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.clear();
        sessions.values().forEach(Session::close);
        clusters.values().forEach(Cluster::close);
        clusters = null;
        sessions = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  private void migrate(String host, String key){
    String keyspace = "migration";
    String psKey = host + ":migration";
    try {
      Session session = sessions.get(host);
      PreparedStatement stmt = updateStmts.get(psKey);
      // Prepare statement on demand
      if (stmt == null) {
        Update updateStmt = QueryBuilder.update(keyspace, keyspace);

        updateStmt.with(QueryBuilder.set("clock", QueryBuilder.bindMarker()));
        updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        stmt = session.prepare(updateStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = updateStmts.putIfAbsent(psKey, stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQueryString());
        logger.debug("key = {}", key);
      }

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      BoundStatement boundStmt = stmt.bind();
      boundStmt.setBytes(vars.size() - 2, ksManager.getMetadata());
      // Add key
      boundStmt.setString(vars.size() - 1, key);

      ksManager.addBlockingDepsCustomPayload(boundStmt);
      //System.out.println(Thread.currentThread().getName() + " | Migrate: " +
      //    ksManager.metadataToString() + " " + host + ":" + keyspace);
      ResultSet execute = session.execute(boundStmt);
      ksManager.incorporateWriteMigrateResponse(execute);

    } catch (Exception e) {
      System.err.println("ERROR MIGRATING: " + e);
      logger.error("Error migrating: {} {}", host, e);
      System.exit(1);

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

    ksManager.nextOp();
    String host = ksManager.currentHost();
    String keyspace = ksManager.currentKeyspace();
    String psKey = host + ":" + keyspace;

    if(ksManager.justMigrated() && (migrate || ksManager.requiresMigration())){
      migrate(ksManager.getPrevHost(), key);
    }

    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get(psKey) : readStmts.get(psKey);

      // Prepare statement on demand
      Session session = sessions.get(host);
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

        stmt = session.prepare(selectBuilder.from(keyspace, table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
            .limit(1));
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
            readAllStmt.putIfAbsent(psKey, stmt) : readStmts.putIfAbsent(psKey, stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQueryString());
      logger.debug("key = {}", key);
      BoundStatement boundStmt = stmt.bind(key);

      ksManager.addBlockingDepsCustomPayload(boundStmt);
      //System.out.println(Thread.currentThread().getName() + " | Read: " + key + " " + ksManager.metadataToString() + " " + host + ":" + keyspace);
      ResultSet rs = session.execute(boundStmt);

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ksManager.incorporateReadResponse(row);

      ColumnDefinitions cd = row.getColumnDefinitions();
      for (ColumnDefinitions.Definition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName(), null);
        }
      }
      if(ksManager.justMigrated()){
        return Status.MIGRATED_OK;
      } else
        return Status.OK;

    } catch (Exception e) {
      System.err.println("ERROR READ: " + e);
      logger.error("Error reading key: {} {}", key, e);
      System.exit(1);
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

    ksManager.nextOp();
    String host = ksManager.currentHost();
    String keyspace = ksManager.currentKeyspace();
    String psKey = host + ":" + keyspace;

    if(ksManager.justMigrated() && (migrate || ksManager.requiresMigration())){
      migrate(ksManager.getPrevHost(), key);
    }

    try {
      Session session = sessions.get(host);
      Set<String> fields = values.keySet();
      PreparedStatement stmt = updateStmts.get(psKey);

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

        PreparedStatement prevStmt = updateStmts.putIfAbsent(psKey, stmt);
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
      boundStmt.setBytes(vars.size() - 2, ksManager.getMetadata());
      // Add key
      boundStmt.setString(vars.size() - 1, key);

      ksManager.addBlockingDepsCustomPayload(boundStmt);
      //System.out.println(Thread.currentThread().getName() + " | Update: " + key + " " + ksManager.metadataToString() + " " + host + ":" + keyspace);
      ResultSet execute = session.execute(boundStmt);
      ksManager.incorporateWriteResponse(execute);

      if(ksManager.justMigrated()){
        return Status.MIGRATED_OK;
      } else
        return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR UPDATE: " + e);
      logger.error("Error updating key: {} {}", key, e);
      System.exit(1);

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

    //No nextOp, since we want to always write in the node's first (primary) partition
    String host = ksManager.currentHost();
    String keyspace = ksManager.currentKeyspace();
    String psKey = host + ":" + keyspace;

    try {
      Session session = sessions.get(host);
      Set<String> fields = values.keySet();
      PreparedStatement stmt = insertStmts.get(psKey);

      // Prepare statement on demand
      if (stmt == null) {
        Insert insertStmt = QueryBuilder.insertInto(keyspace, table);
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

        PreparedStatement prevStmt = insertStmts.putIfAbsent(psKey, stmt);
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
      boundStmt.setBytes(vars.size() - 1, ksManager.getMetadata());
      ksManager.addBlockingDepsCustomPayload(boundStmt);
      //System.out.println(Thread.currentThread().getName() + " | Insert: " + key + " " + ksManager.metadataToString() + " " + host + ":" + keyspace);
      ResultSet execute = session.execute(boundStmt);
      ksManager.incorporateWriteResponse(execute);
      return Status.OK;

    } catch (Exception e) {
      System.err.println("ERROR INSERT: " + e);
      logger.error("Error inserting key: {} {}", key, e);
      System.exit(1);

    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String t, String sk, int rc, Set<String> f, Vector<HashMap<String, ByteIterator>> r) {
    throw new AssertionError("Scan not implemented");
  }

  @Override
  public Status delete(String table, String key) {
    throw new AssertionError("Delete not implemented");
  }

  private enum KsManager {
    regular, visibility
  }

}
