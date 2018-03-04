package de.uni_leipzig.dbs.randomforest

import org.scalatest.{FlatSpec, Matchers}

class DecisionTreeTrainerTest extends FlatSpec with Matchers {

  val lfList1 = LabeledFeatures(1, Vector(1,1)) ::
  LabeledFeatures(1, Vector(1,2)) ::
  LabeledFeatures(1, Vector(2,1)) ::
  LabeledFeatures(1, Vector(2,4)) ::
  LabeledFeatures(2, Vector(6,5)) ::
  LabeledFeatures(2, Vector(5,6)) ::
  LabeledFeatures(2, Vector(8,6)) ::
  LabeledFeatures(3, Vector(11,20)) ::
  LabeledFeatures(3, Vector(8,8)) ::
  LabeledFeatures(3, Vector(8,10)) :: Nil

  "A decisionTreeTrainer" should "produce the right tree" in {
    val decisionTreeTrainer = new DecisionTreeTrainer()
    val tree = decisionTreeTrainer.train(lfList1)
    println(tree)
  }
}
