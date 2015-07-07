package spark.util

import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps

import spark.{SparkEnv, SparkConf}
import spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}
object RpcUtils {

  /**
   * Retrieve a [[RpcEndpointRef]] which is located in the driver via its name.
   */
  def makeDriverRef(name: String, conf: SparkConf, rpcEnv: RpcEnv): RpcEndpointRef = {
    val driverActorSystemName = SparkEnv.driverActorSystemName
    val driverHost: String = conf.get("spark.driver.host", "localhost")
    val driverPort: Int = conf.getInt("spark.driver.port", 7077)
    rpcEnv.setupEndpointRef(driverActorSystemName, RpcAddress(driverHost, driverPort), name)
  }

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: SparkConf): Int = {
    conf.getInt("spark.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: SparkConf): Long = {
    conf.getTimeAsMs("spark.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  private[spark] def askRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.askTimeout", "spark.network.timeout"), "120s")
  }

  @deprecated("use askRpcTimeout instead, this method was not intended to be public", "1.5.0")
  def askTimeout(conf: SparkConf): FiniteDuration = {
    askRpcTimeout(conf).duration
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  private[spark] def lookupRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.lookupTimeout", "spark.network.timeout"), "120s")
  }

  @deprecated("use lookupRpcTimeout instead, this method was not intended to be public", "1.5.0")
  def lookupTimeout(conf: SparkConf): FiniteDuration = {
    lookupRpcTimeout(conf).duration
  }
}