package de.uni_leipzig.dbs.tree

case class Split(
           val leftChild: Node,
           val rightChild: Node,
           val featureIndex: Int,
           val threshold: Double
           ) extends Serializable
