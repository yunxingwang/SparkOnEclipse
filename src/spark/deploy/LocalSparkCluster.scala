package spark.deploy

import akka.actor.{ActorRef, Props, Actor, ActorSystem, Terminated}

import spark.deploy.worker.Worker
import spark.deploy.master.Master
import spark.util.AkkaUtils
import spark.Logging
import spark.util.Utils
import spark.SparkConf

import scala.collection.mutable.ArrayBuffer

private[spark]
class LocalSparkCluster(numSlaves: Int, coresPerSlave: Int, memoryPerSlave: Int) extends Logging {
  
  val localIpAddress = Utils.localIpAddress
  
  var masterActor : ActorRef = _
  var masterActorSystem : ActorSystem = _
  var masterPort : Int = _
  var masterUrl : String = _
  
  val slaveActorSystems = ArrayBuffer[ActorSystem]()
  val slaveActors = ArrayBuffer[ActorRef]()
  
  def start() : String = {
    logInfo("Starting a local Spark cluster with " + numSlaves + " slaves.")
    val conf=new SparkConf
    /* Start the Master */
    val (actorSystem, masterPort) = AkkaUtils.createActorSystem("sparkMaster", localIpAddress, 0, conf)
    masterActorSystem = actorSystem
    masterUrl = "spark://" + localIpAddress + ":" + masterPort
    val actor = masterActorSystem.actorOf(
      Props(new Master(localIpAddress, masterPort, conf)), name = "Master")
    masterActor = actor

    /* Start the Slaves */
    for (slaveNum <- 1 to numSlaves) {
      val (actorSystem, boundPort) = 
        AkkaUtils.createActorSystem("sparkWorker" + slaveNum, localIpAddress, 0,conf)
      slaveActorSystems += actorSystem
      val actor = actorSystem.actorOf(
        Props(new Worker(localIpAddress, boundPort, coresPerSlave, memoryPerSlave, masterUrl)),
        name = "Worker")
      slaveActors += actor
    }

    return masterUrl
  }

  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the slaves before the master so they don't get upset that it disconnected
    slaveActorSystems.foreach(_.shutdown())
    slaveActorSystems.foreach(_.awaitTermination())
    masterActorSystem.shutdown()
    masterActorSystem.awaitTermination()
  }
}
