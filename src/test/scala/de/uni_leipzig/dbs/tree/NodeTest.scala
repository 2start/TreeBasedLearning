package de.uni_leipzig.dbs.tree

import org.scalatest.{FlatSpec, Matchers}

class NodeTest extends FlatSpec with Matchers {
  /**
    * Tree Visualisation
    *
    * Root1
    * <= 5.00
    * Node2       Leaf3
    * <= 4.00     2.00
    * Leaf4   Leaf5
    * 0.00    1.00
    */

  "A node" should "predict the right value" in {
    val leaf3 = new Node(3, NodeStatistics(Map(2.0 -> 5, 1.0 -> 3)), None)
    val leaf4 = new Node(4, NodeStatistics(Map(0.0 -> 5, 1.0 -> 0, 2.0 -> 3)), None)
    val leaf5 = new Node(5, NodeStatistics(Map(0.0 -> 1, 1.0 -> 5)), None)
    val node2 = new Node(2, NodeStatistics(Map(3.0 -> 4)), Some(Split(leaf4, leaf5, 1, 4.0)))
    val rootNode = new Node(1, NodeStatistics(Map(1.0 -> 5)), Some(Split(node2, leaf3, 0, 5.0)))

    rootNode.predict(Vector(1.0, 1.0)) should be(0.0)
    rootNode.predict(Vector(1.0, 5.0)) should be(1.0)
    rootNode.predict(Vector(5.0, 5.0)) should be(1.0)
    rootNode.predict(Vector(6.0, 6.0)) should be(2.0)
  }
}
