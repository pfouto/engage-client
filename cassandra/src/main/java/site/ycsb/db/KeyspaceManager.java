package site.ycsb.db;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

public class KeyspaceManager {

  private final int nOpsLocal;
  private final int nOpsRemote;
  private final String localHost;

  private final List<String> keyspacesLocal;
  private final List<String> keyspacesRemote;
  private final Map<String, Set<String>> hostsKeyspaces;
  private final Random r;
  private final Map<Inet4Address, Integer> readClock;
  private final Map<Inet4Address, Integer> writeClock;
  private final SessionGuarantees guarantees;
  private final Protocol protocol;
  private String currentKs;
  private String currentHost;
  private String prevHost;
  private int currentOpCount;
  private boolean local;
  private Map.Entry<Inet4Address, Integer> saturnLabel;
  private boolean migrated;

  public KeyspaceManager(String localHost, List<String> keyspacesLocal, Set<String> keyspacesRemote,
                         Map<String, Set<String>> hostsKeyspaces, int nOpsLocal, int nOpsRemote,
                         String protocol, String guarantees) throws UnknownHostException {
    this.localHost = localHost;
    this.keyspacesLocal = keyspacesLocal;
    this.keyspacesRemote = new ArrayList<>(keyspacesRemote);
    this.hostsKeyspaces = hostsKeyspaces;
    this.nOpsLocal = nOpsLocal;
    this.nOpsRemote = nOpsRemote;
    this.guarantees = SessionGuarantees.valueOf(guarantees);
    this.protocol = Protocol.valueOf(protocol);
    this.migrated = false;

    readClock = new HashMap<>();
    writeClock = new HashMap<>();
    saturnLabel = new AbstractMap.SimpleEntry<>((Inet4Address) InetAddress.getLocalHost(), -1);

    currentOpCount = 0;
    currentHost = localHost;
    currentKs = this.keyspacesLocal.get(0);
    local = true;

    r = new Random();
    //System.err.println("Initial host " + currentHost);
  }

  void addBlockingDepsCustomPayload(BoundStatement boundStmt) {
    Map<String, ByteBuffer> customPayload = new HashMap<>();

    if (protocol == Protocol.saturn) {
      if (saturnLabel != null)
        customPayload.put("c", entryToBuffer(saturnLabel));
    } else {
      if (guarantees == SessionGuarantees.WFR || guarantees == SessionGuarantees.MR)
        customPayload.put("c", clockToBuffer(readClock));
      else if (guarantees == SessionGuarantees.MW || guarantees == SessionGuarantees.RYW)
        customPayload.put("c", clockToBuffer(writeClock));
      else if (guarantees == SessionGuarantees.CAUSAL)
        customPayload.put("c", clockToBuffer(mergedClocks(readClock, writeClock)));
    }
    boundStmt.setOutgoingPayload(customPayload);
  }

  public ByteBuffer getMetadata() {
    return protocol == Protocol.saturn ? entryToBuffer(saturnLabel) : clockToBuffer(mergedClocks(readClock, writeClock));
  }

  public String metadataToString() {
    return protocol == Protocol.saturn ? saturnLabel.toString() : readClock + " " + writeClock;
  }

  void incorporateWriteResponse(ResultSet execute) throws UnknownHostException {
    ByteBuffer c = execute.getExecutionInfo().getIncomingPayload().get("c");
    byte[] addressBytes = new byte[4];
    c.get(addressBytes);
    InetAddress address = Inet4Address.getByAddress(addressBytes);
    int val = c.getInt();
    if (protocol == Protocol.saturn)
      saturnLabel = new AbstractMap.SimpleEntry<>((Inet4Address) address, val);
    else
      writeClock.merge((Inet4Address) address, val, Math::max);
  }

  void incorporateWriteMigrateResponse(ResultSet execute) throws UnknownHostException {
    if (protocol != Protocol.saturn) return;
    ByteBuffer c = execute.getExecutionInfo().getIncomingPayload().get("c");
    byte[] addressBytes = new byte[4];
    c.get(addressBytes);
    InetAddress address = Inet4Address.getByAddress(addressBytes);
    int val = c.getInt();
    saturnLabel = new AbstractMap.SimpleEntry<>((Inet4Address) address, val);
  }

  void incorporateReadResponse(Row row) throws IOException {
    byte[] clockData = row.getBytes("clock").array();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(clockData));
    if (protocol == Protocol.saturn) {
      Inet4Address addr = (Inet4Address) Inet4Address.getByAddress(dis.readNBytes(4));
      int val = dis.readInt();
      if (val > saturnLabel.getValue() || (val == saturnLabel.getValue() && greater(addr, saturnLabel.getKey())))
        saturnLabel = new AbstractMap.SimpleEntry<>(addr, val);
    } else {
      int size = dis.readInt();
      for (int i = 0; i < size; i++) {
        Inet4Address addr = (Inet4Address) Inet4Address.getByAddress(dis.readNBytes(4));
        int val = dis.readInt();
        readClock.merge(addr, val, Math::max);
      }
    }
  }

  public void nextOp() {
    //TODO migrate local/remote, then home, then local/remote (based on percentage)
    currentOpCount++;
    migrated = false;
    if (local && currentOpCount > nOpsLocal && nOpsRemote > 0) {
      currentKs = keyspacesRemote.get(r.nextInt(keyspacesRemote.size()));
      prevHost = currentHost;
      currentHost = null;

      List<Map.Entry<String, Set<String>>> entries = new LinkedList<>(hostsKeyspaces.entrySet());
      Collections.shuffle(entries);
      for (Map.Entry<String, Set<String>> entry : entries) {
        String h = entry.getKey();
        if (h.equals(localHost)) continue;
        Set<String> parts = entry.getValue();
        if (parts.contains(currentKs)) {
          currentHost = h;
          //System.out.println(Thread.currentThread().getName() + " | New host " + currentHost);
          break;
        }
      }
      if (currentHost == null)
        throw new AssertionError("Did not find host for partition " + currentKs);

      currentOpCount = 0;
      local = false;
      migrated = true;
    } else if (!local && currentOpCount > nOpsRemote) {
      prevHost = currentHost;
      currentHost = localHost;
      //System.out.println(Thread.currentThread().getName() + " | New host " + currentHost);
      currentKs = keyspacesLocal.get(r.nextInt(keyspacesLocal.size()));
      currentOpCount = 0;
      local = true;
      migrated = true;
    }
  }

  public boolean justMigrated() {
    return migrated;
  }

  public String currentKeyspace() {
    return currentKs;
  }

  public String currentHost() {
    return currentHost;
  }

  public String getPrevHost() {
    return prevHost;
  }

  private ByteBuffer clockToBuffer(Map<Inet4Address, Integer> clock) {
    ByteBuffer allocate = ByteBuffer.allocate((clock.size() * 2 + 1) * 4);
    allocate.putInt(clock.size());
    clock.forEach((k, v) -> {
      allocate.put(k.getAddress());
      allocate.putInt(v);
    });
    allocate.flip();
    return allocate;
  }

  private ByteBuffer entryToBuffer(Map.Entry<Inet4Address, Integer> clockEntry) {
    ByteBuffer allocate = ByteBuffer.allocate(8);
    allocate.put(clockEntry.getKey().getAddress());
    allocate.putInt(clockEntry.getValue());
    allocate.flip();
    return allocate;
  }

  private Map<Inet4Address, Integer> mergedClocks(Map<Inet4Address, Integer> rc, Map<Inet4Address, Integer> wc) {
    Map<Inet4Address, Integer> merged = new HashMap<>(rc);
    wc.forEach((k, v) -> merged.merge(k, v, Math::max));
    return merged;
  }

  private boolean greater(Inet4Address adr1, Inet4Address adr2) {
    byte[] ba1 = adr1.getAddress();
    byte[] ba2 = adr2.getAddress();

    // we have 2 ips of the same type, so we have to compare each byte
    for (int i = 0; i < ba1.length; i++) {
      int b1 = unsignedByteToInt(ba1[i]);
      int b2 = unsignedByteToInt(ba2[i]);
      if (b1 != b2) return b1 > b2;
    }
    return false;
  }

  private int unsignedByteToInt(byte b) {
    return (int) b & 0xFF;
  }

  private enum SessionGuarantees {
    RYW(1), MR(2), WFR(3), MW(4), CAUSAL(5);

    private final int value;

    SessionGuarantees(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  private enum Protocol {
    saturn, engage, edgegage, bayou;
  }
}
