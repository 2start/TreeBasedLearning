package de.uni_leipzig.dbs

import org.apache.flink.api.scala.DataSet

import scala.reflect.ClassTag

object Util {

    def javaDataSetToScalaDataSet[T: ClassTag](javaData: org.apache.flink.api.java.DataSet[T]): DataSet[T] ={
      new DataSet[T](javaData)
    }

    def scalaDataSetToJavaDataSet[T](data: DataSet[T]): org.apache.flink.api.java.DataSet[T] = {
      val javaSet = data.getClass.getDeclaredMethod("javaSet")
      javaSet.setAccessible(true)
      javaSet.invoke(data).asInstanceOf[org.apache.flink.api.java.DataSet[T]]
    }

}
