package site.ycsb.db;

import java.util.List;
import java.util.Map;

public class Tree {

  private final Map<String, NodeInfo> nodes;
  private final List<List<String>> links;

  public Tree(Map<String, NodeInfo> nodes, List<List<String>> links) {
    this.nodes = nodes;
    this.links = links;
  }

  public List<List<String>> getLinks() {
    return links;
  }

  public Map<String, NodeInfo> getNodes() {
    return nodes;
  }
}