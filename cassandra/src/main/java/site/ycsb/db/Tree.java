package site.ycsb.db;

import java.util.List;
import java.util.Map;

public class Tree {

  private final Map<String, PartitionInfo> nodes;
  private final List<List<String>> links;

  public Tree(Map<String, PartitionInfo> nodes, List<List<String>> links) {
    this.nodes = nodes;
    this.links = links;
  }

  public List<List<String>> getLinks() {
    return links;
  }

  public Map<String, PartitionInfo> getNodes() {
    return nodes;
  }
}