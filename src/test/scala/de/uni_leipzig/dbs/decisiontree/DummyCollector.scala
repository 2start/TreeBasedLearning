package de.uni_leipzig.dbs.decisiontree

import de.uni_leipzig.dbs.tree.SplitInformation
import org.apache.flink.util.Collector


class DummyCollector extends Collector[SplitInformation]{
  override def collect(t: SplitInformation): Unit = {
    println(t)
  }

  override def close(): Unit = {}
}
