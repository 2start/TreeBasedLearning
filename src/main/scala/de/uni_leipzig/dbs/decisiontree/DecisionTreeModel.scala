package de.uni_leipzig.dbs.decisiontree

import org.apache.flink.api.scala._

class DecisionTreeModel(
                         val maxDepth: Int = Int.MaxValue,
                         val minLeafSamples: Int = 1,
                         val minSplitGain: Double = 0
                       ) extends Evaluatable with Serializable{

  var rootNode: Node = null

  def fit(data: DataSet[(Double, Vector[Double])]): DecisionTreeModel = {
    val dtt = new DecisionTreeTrainer
    rootNode = dtt.createTree(data)
    this
  }

  def predict(data: DataSet[Vector[Double]]): DataSet[(Double, Vector[Double])] = {
    data.map(features => (rootNode.predict(features), features))
  }
}

