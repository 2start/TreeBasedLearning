package de.uni_leipzig.dbs

import org.scalatest.{FlatSpec, Matchers}

class NodeTest extends FlatSpec with Matchers{
  /**
    *   Tree Visualisation
    *
    *           Root1
    *          <= 5.00
    *     Node2       Leaf3
    *    <= 4.00     2.00
    * Leaf4   Leaf5
    * 0.00    1.00
    */

  "A node" should "predict the right " in {
    val leaf3 = new Node(3, 2.0, 1.0, None, None, None, None, true)
    val leaf4 = new Node(4, 0.0, 1.0, None, None, None, None, true)
    val leaf5 = new Node(5, 1.0, 1.0, None, None, None, None, true)
    val node2 = new Node(2, 0.0, 0.0, Some(1), Some(4.0), Some(leaf4), Some(leaf5), false)
    val rootNode = new Node(1, 0.0, 0.0, Some(0), Some(5.0), Some(node2), Some(leaf3), false)

    rootNode.predict(Vector(1.0, 1.0)) should be (0.0)
    rootNode.predict(Vector(1.0, 5.0)) should be (1.0)
    rootNode.predict(Vector(5.0, 5.0)) should be (1.0)
    rootNode.predict(Vector(6.0, 6.0)) should be (2.0)
  }
}
