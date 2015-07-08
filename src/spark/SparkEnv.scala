package spark

import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.remote.RemoteActorRefProvider
import serializer.Serializer
import spark.util.AkkaUtils

/**
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a thread-local variable, so each thread that accesses these
 * objects needs to have the right SparkEnv set. You can get the current environment with
 * SparkEnv.get (e.g. after creating a SparkContext) and set it with SparkEnv.set.
 */
class SparkEnv(
    val actorSystem: ActorSystem,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheTracker: CacheTracker,
    val mapOutputTracker: MapOutputTracker) {

  /** No-parameter constructor for unit tests. */
  def this() = {
    this(null, new JavaSerializer, new JavaSerializer, null, null)
  }

  def stop() {
    actorSystem.shutdown()
    // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
    // down, but let's call it anyway in case it gets fixed in a later release
    actorSystem.awaitTermination()
  }
}

object SparkEnv extends Logging {
  private val env = new ThreadLocal[SparkEnv]
  private[spark] val executorActorSystemName = "sparkExecutor"
  private[spark] val driverActorSystemName = "sparkDriver"
  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(
    hostname: String,
    port: Int,
    isMaster: Boolean,
    isLocal: Boolean): SparkEnv = {
    val conf = new SparkConf
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, port, conf)

    // Bit of a hack: If this is the master and our port was 0 (meaning bind to any free port),
    // figure out which port number Akka actually bound to and set spark.master.port to it.
    if (isMaster && port == 0) {
      System.setProperty("spark.master.port", boundPort.toString)
    }

    val classLoader = Thread.currentThread.getContextClassLoader

    // Create an instance of the class named by the given Java system property, or by
    // defaultClassName if the property is not set, and return it as a T
    def instantiateClass[T](propertyName: String, defaultClassName: String): T = {
      val name = System.getProperty(propertyName, defaultClassName)
      Class.forName(name, true, classLoader).newInstance().asInstanceOf[T]
    }

    val serializer = instantiateClass[Serializer]("spark.serializer", "spark.JavaSerializer")

    val closureSerializer = instantiateClass[Serializer](
      "spark.closure.serializer", "spark.JavaSerializer")
    val cacheTracker = new CacheTracker(actorSystem, isMaster)
    // Warn about deprecated spark.cache.class property
    if (System.getProperty("spark.cache.class") != null) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
        "levels using the RDD.persist() method instead.")
    }
    val mapOutputTracker = new MapOutputTracker(actorSystem, isMaster)

    new SparkEnv(
      actorSystem,
      serializer,
      closureSerializer, cacheTracker, mapOutputTracker)
  }
}
