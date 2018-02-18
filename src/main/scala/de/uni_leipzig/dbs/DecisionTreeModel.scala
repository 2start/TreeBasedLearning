package de.uni_leipzig.dbs
import org.apache.flink.api.scala._

class DecisionTreeModel extends Evaluatable with Serializable {
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

