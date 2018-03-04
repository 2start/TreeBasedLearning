package de.uni_leipzig.dbs.tree

import org.scalatest.{FlatSpec, Matchers}

class NodeStatisticsTest extends FlatSpec with Matchers{
  val nodeStatistics1 = NodeStatistics(Map(3.0 -> 4, 2.0 -> 5, 1.0 -> 8))
  val emptyStats = NodeStatistics(Map())

  "A NodeStatistics Instance" should "calculate the proper entropy" in {
    nodeStatistics1.entropy should be (1.52218987217 +- 0.001)
    emptyStats.entropy should be (0)
  }

  it should "find the predicted value" in {
   nodeStatistics1.prediction should be (1.0)
  }

  it should "calculate the right label probability" in {
    nodeStatistics1.labelProbability(1.0) should be (8.0/17.0)
  }
}
