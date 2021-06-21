package site.ycsb.db;

import java.util.List;

public class PartitionInfo {

  private final boolean local_db;
  private final List<String> partitions;

  public PartitionInfo(boolean local_db, List<String> partitions) {
    this.local_db = local_db;
    this.partitions = partitions;
  }

  public boolean isLocal_db() {
    return local_db;
  }

  public List<String> getPartitions() {
    return partitions;
  }
}
