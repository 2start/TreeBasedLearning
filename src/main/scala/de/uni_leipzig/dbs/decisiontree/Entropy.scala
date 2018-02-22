package de.uni_leipzig.dbs.decisiontree

case class Entropy(
                val totalCount: Long,
                val labelCountMap: Map[Double, Int],
                val entropy: Double
)
