package site.ycsb.db;

import java.util.List;

public class NodeInfo {

  private final boolean local_db;
  private final boolean up;
  private final int region;
  private final List<String> partitions;

  public NodeInfo(boolean local_db, boolean up, int region, List<String> partitions) {
    this.local_db = local_db;
    this.partitions = partitions;
    this.up = up;
    this.region = region;
  }

  public boolean isLocal_db() {
    return local_db;
  }

  public boolean isUp() {
    return up;
  }

  public int getRegion() {
    return region;
  }

  public List<String> getPartitions() {
    return partitions;
  }
}
