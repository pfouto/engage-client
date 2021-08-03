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

public class KeyspaceManagerVisib implements KeyspaceManager{

  private final List<String> keyspacesLocal;
  private final Random r;
  private final Map<Inet4Address, Integer> readClock;
  private final Map<Inet4Address, Integer> writeClock;
  private final SessionGuarantees guarantees;
  private final Protocol protocol;
  private String currentKs;
  private final String currentHost;
  private Map.Entry<Inet4Address, Integer> saturnLabel;

  public KeyspaceManagerVisib(String localHost, List<String> keyspacesLocal, String protocol, String guarantees)
      throws UnknownHostException {
    this.keyspacesLocal = keyspacesLocal;
    this.guarantees = SessionGuarantees.valueOf(guarantees.toUpperCase());
    this.protocol = Protocol.valueOf(protocol);

    readClock = new HashMap<>();
    writeClock = new HashMap<>();
    saturnLabel = new AbstractMap.SimpleEntry<>((Inet4Address) InetAddress.getLocalHost(), -1);

    currentHost = localHost;
    currentKs = this.keyspacesLocal.get(0);

    r = new Random();
    //System.err.println("Initial host " + currentHost);
  }

  public void addBlockingDepsCustomPayload(BoundStatement boundStmt) {
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

  @Override
  public boolean requiresMigration() {
    return protocol == Protocol.saturn;
  }

  public void incorporateWriteResponse(ResultSet execute) throws UnknownHostException {
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

  public void incorporateWriteMigrateResponse(ResultSet execute) throws UnknownHostException {
    if (protocol != Protocol.saturn) return;
    ByteBuffer c = execute.getExecutionInfo().getIncomingPayload().get("c");
    byte[] addressBytes = new byte[4];
    c.get(addressBytes);
    InetAddress address = Inet4Address.getByAddress(addressBytes);
    int val = c.getInt();
    saturnLabel = new AbstractMap.SimpleEntry<>((Inet4Address) address, val);
  }

  public void incorporateReadResponse(Row row) throws IOException {
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
      currentKs = keyspacesLocal.get(r.nextInt(keyspacesLocal.size()));
  }

  public boolean justMigrated() {
    return false;
  }

  public String currentKeyspace() {
    return currentKs;
  }

  public String currentHost() {
    return currentHost;
  }

  public String getPrevHost() {
    return currentHost;
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

}
