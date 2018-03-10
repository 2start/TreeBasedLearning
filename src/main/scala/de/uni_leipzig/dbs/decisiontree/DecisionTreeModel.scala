package de.uni_leipzig.dbs.decisiontree

import de.uni_leipzig.dbs.Evaluatable
import de.uni_leipzig.dbs.tree.Node
import org.apache.flink.api.scala._

class DecisionTreeModel(
                         val maxDepth: Int = Int.MaxValue,
                         val minLeafSamples: Int = 1,
                         val minImpurityDecrease: Double = 0
                       ) extends Evaluatable with Serializable {

  var rootNode: Node = null

  def fit(data: DataSet[(Double, Vector[Double])]): DecisionTreeModel = {
    val dtt = new DecisionTreeTrainer(maxDepth = maxDepth, minLeafSamples = minLeafSamples, minImpurityDecrease = minImpurityDecrease)
    rootNode = dtt.createTree(data)
    this
  }

  def predict(data: DataSet[Vector[Double]]): DataSet[(Double, Vector[Double])] = {
    require(rootNode != null)
    data.map(features => (rootNode.predict(features), features))
  }
}

