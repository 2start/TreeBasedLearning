package de.uni_leipzig.dbs.decisiontree

import org.apache.flink.util.Collector
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConverters._

class BestSplitTest extends FlatSpec with Matchers{



  behavior of "featureSplitInformationCalculator"

  it should "work" in {
    val fs = new FeatureSplitInformationCalculator()

    val list = List(
      (1, 0.0, 0.0),
      (1, 0.0, 0.0),
      (1, 0.0, 0.0),
      (1, 0.0, 0.0),
      (1, 1.0, 0.0),
      (1, 1.0, 0.0),
      (1, 1.0, 0.0),
      (1, 1.0, 0.0)



    ).asJava
    fs.reduce(list, new DummyCollector)
  }

}
