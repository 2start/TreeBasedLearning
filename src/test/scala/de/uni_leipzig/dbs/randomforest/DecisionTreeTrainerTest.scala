package de.uni_leipzig.dbs.randomforest

import org.scalatest.{FlatSpec, Matchers}

class DecisionTreeTrainerTest extends FlatSpec with Matchers {

  "A decisionTreeTrainer" should "handle a small dataset" in {
    val lflist1 = LabeledFeatures(1, Vector(1, 1)) ::
      LabeledFeatures(1, Vector(1, 2)) ::
      LabeledFeatures(1, Vector(2, 1)) ::
      LabeledFeatures(1, Vector(2, 4)) ::
      LabeledFeatures(2, Vector(6, 5)) ::
      LabeledFeatures(2, Vector(5, 6)) ::
      LabeledFeatures(2, Vector(8, 6)) ::
      LabeledFeatures(3, Vector(11, 20)) ::
      LabeledFeatures(3, Vector(8, 8)) ::
      LabeledFeatures(3, Vector(8, 10)) :: Nil
    val decisionTreeTrainer = new DecisionTreeTrainer()
    val tree = decisionTreeTrainer.createTree(lflist1)
    println(tree)
  }

  "A decisionTreeTrainer" should "handle a big dataset" in {


  val bufferedSource = scala.io.Source.fromFile(getClass.getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_4].csv").toURI)
    // skips header
    val linesWithHeader = bufferedSource.getLines().drop(1)
    val lines = linesWithHeader.map(_.split("\\;").map(_.trim)).toList
    val labeledFeaturesList = lines.map(lineAttributes => {
      val label = if (lineAttributes(2).toBoolean) 1.0 else -1.0
      LabeledFeatures(label, lineAttributes.slice(3, lineAttributes.length).map(_.toDouble).toVector)
    })

    val decisionTreeTrainer = new DecisionTreeTrainer(featuresPerSplit = 3, minLeafSamples = 500)
    val tree = decisionTreeTrainer.createTree(labeledFeaturesList)
    println(tree)
  }


}
