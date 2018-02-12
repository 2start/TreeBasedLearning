package de.uni_leipzig.dbs

import java.lang

import org.apache.flink.api.common.functions.{RichFilterFunction, RichGroupReduceFunction, RichMapFunction}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

class DecisionTreeTrainer {

}

