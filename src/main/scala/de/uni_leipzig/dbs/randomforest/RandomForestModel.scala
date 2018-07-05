package de.uni_leipzig.dbs.randomforest

import de.uni_leipzig.dbs.Evaluatable
import de.uni_leipzig.dbs.tree.Node
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.util.Collector
//import org.apache.flink.api.scala.utils.DataSetUtils

/***
  * The model of the random forest algorithm.
  *
  * @param sampleFraction
  * @param numTrees
  * @param minImpurityDecrease
  * @param minLeafSamples
  * @param maxDepth
  * @param featuresPerSplit
  */
class RandomForestModel(
                         val sampleFraction: Double = 0.05,
                         val numTrees: Int = 100,
                         val minImpurityDecrease: Double = 0.00,
                         val minLeafSamples: Int = 1,
                         val maxDepth: Int = Int.MaxValue,
                         val featuresPerSplit: Int = 1
                       ) extends Evaluatable with Serializable {
 require(0.0 < sampleFraction && sampleFraction <= 1.0)
  var trees: List[Node] = _

  /***
    * Fits the random forest model to the data.
    * @param data
    */
  def fit(data: DataSet[LabeledFeatures]): Unit = {
    val dt = new DecisionTreeTrainer(minImpurityDecrease, minLeafSamples, featuresPerSplit, maxDepth)
    trees = data
      .flatMap(new Bootstrapper(sampleFraction, numTrees))
      .groupingBy { case (tree, labeledFeatures) => tree }
      .reduceGroup(it => {
        val labeledFeaturesList = it.toList.map { case (tree, labeledFeatures) => labeledFeatures }
        val rootNode = dt.createTree(labeledFeaturesList)
        rootNode
      }).collect().toList
  }

  /***
    * Predicts a set of unlabeled data points. Requires to fit the model first.
    * @param data represents the data that should be predicted.
    * @return
    */
  def predictLabeledFeatures(data: DataSet[Vector[Double]]): DataSet[LabeledFeatures] = {
    require(trees.nonEmpty)
    data.map(features => {
      // looking for the most commonly assigned label
      // serialized trees are sent to the workers to predict the labels
      val label = trees.map(_.predict(features)).groupBy(identity).mapValues(_.size).maxBy(_._2)._1
      LabeledFeatures(label, features)
    })
  }

  /***
    * Predicts a set of unlabeled data points. Requires to fit the model first.
    * @param data represents the data that should be predicted.
    * @return
    */
  // TODO change decision tree to accept LabeledFeatures instead of (label, features) then edit Evaluatable and remove this
  def predict(data: DataSet[Vector[Double]]): DataSet[(Double, Vector[Double])] = {
    predictLabeledFeatures(data).map(lv => (lv.label, lv.features))
  }

  /**
    * A simple variable importance measure.
    * @return map from feature index to average information gain
    */
  def getVariableImportances(): Map[Int, Double] = {
    /**
      *
      * @param node that represents the examined tree
      * @return featureIndices and impurity decrease for every non leaf node
      */
    def getFeatureInformationGain(node: Node, totalCount: Int): List[(Int, Double)] = {
      if(node.split.isEmpty) {
        return Nil
      } else {
        val split = node.split.get
        val featureIndex = split.featureIndex
        val entropy = node.stats.entropy
        val leftChild = split.leftChild
        val rightChild = split.rightChild
        val leftChildEntropy = leftChild.stats.entropy
        val rightChildEntropy = rightChild.stats.entropy
        val leftChildCount = leftChild.stats.count
        val rightChildCount = rightChild.stats.count
        val totalCount = leftChildCount + rightChildCount
        val leftChildWeightedEntropy = leftChildCount/totalCount * leftChildEntropy
        val rightChildWeightedEntropy = rightChildCount/totalCount * rightChildEntropy
        val informationGain = entropy - leftChildWeightedEntropy - rightChildWeightedEntropy
        val weightedInformationGain = node.stats.count.toDouble/totalCount * informationGain

        return List((featureIndex, weightedInformationGain)) ++ getFeatureInformationGain(leftChild, totalCount) ++ getFeatureInformationGain(rightChild, totalCount)

      }
    }

    // calculates (feature index, average information gain) out of all nodes (feature index, information gain) tuples
    val featureImportanceMap = trees
      .flatMap(node => getFeatureInformationGain(node, node.stats.count))
      .groupBy{case(featureIndex, _) => featureIndex}
      .mapValues{groupedTuples => {
        val featureInformationGains = groupedTuples.map{case(_, informationGain) => informationGain}
        featureInformationGains.sum/trees.size.toDouble
      }}

    return featureImportanceMap
  }
}

class Bootstrapper(fraction: Double, numTrees: Int) extends RichFlatMapFunction[LabeledFeatures, (Int, LabeledFeatures)] {
  override def flatMap(in: LabeledFeatures, collector: Collector[(Int, LabeledFeatures)]): Unit = {
    1 to numTrees map { tree =>
      val rnd = math.random
      if (rnd <= fraction) {
        collector.collect((tree, in))
      }
    }
  }
}


