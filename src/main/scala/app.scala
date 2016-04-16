/**
 * Created by nperez on 4/15/16.
 */

package com.nico

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{SparkConf, SparkContext}



object app {

  def main(args: Array[String]) {

    val port = args(0).toInt

    val config = new SparkConf().setAppName("hive")

    val sc = new SparkContext(config)

    val ssc = new StreamingContext(sc, Seconds(20))

    val stream = ssc.receiverStream(new OrderReceiver(port))
    stream.start()

    stream.foreachRDD { rdd =>
      rdd.foreach(order => println(order))
    }

    println("batching")

    ssc.awaitTermination()
  }
}

case class Dog(name: String, age: Int)

case class Order(id: Int, total: Int, items: List[Item] = null)

case class Item(id: Int, cost: Int)

class OrderReceiver(port: Int) extends Receiver[Order](StorageLevel.MEMORY_ONLY)  {

  onStart()

  override def onStart(): Unit = {

    println("starting...")


    val thread = new Thread("Receiver") {
      override def run() {receive() }
    }

    thread.start()
  }

  override def onStop(): Unit = stop("Done")


  def receive() = {
    val socket = new Socket("127.0.0.1", port)
    var currentOrder: Order = null
    var currentItems: List[Item] = null

    val reader = new BufferedReader(new InputStreamReader (socket.getInputStream(), "UTF-8"))

    while (!isStopped()) {
      var userInput = reader.readLine()

      if (userInput == null) stop("Stream has ended")

      else {
        val parts = userInput.split(" ")

        if (parts.length == 2) {
          if (currentOrder != null) {
            store(Order(currentOrder.id, currentOrder.total, currentItems))
          }

          currentOrder = Order(parts(0).toInt, parts(1).toInt)
          currentItems = List[Item]()
        }
        else {
          currentItems = Item(parts(0).toInt, parts(1).toInt) :: currentItems
        }
      }
    }
  }
}