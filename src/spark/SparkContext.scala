package spark
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap
import spark.scheduler.local.LocalScheduler
import scala.reflect.ClassTag
import spark.scheduler.TaskScheduler
/**
 * 先不接触文件中的数据，计算pi或者从内存生成数据
 * @author guotong
 */
class SparkContext(master: String, frameworkName: String) {
  private var nextRddId = new AtomicInteger(0)
  private[spark] val addedFiles = HashMap[String, Long]()
  private[spark] val addedJars = HashMap[String, Long]()
  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()
   // Create and start the scheduler
  private var taskScheduler: TaskScheduler = {
    // Regular expression used for local[N] master format
    val LOCAL_N_REGEX = """local\[([0-9]+)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """(spark://.*)""".r

    master match {
      case "local" =>
        new LocalScheduler(1, 0, this)

      case LOCAL_N_REGEX(threads) =>
        new LocalScheduler(threads.toInt, 0, this)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        new LocalScheduler(threads.toInt, maxFailures.toInt, this)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new ClusterScheduler(this)
        val backend = new SparkDeploySchedulerBackend(scheduler, this, sparkUrl, jobName)
        scheduler.initialize(backend)
        scheduler

      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure SPARK_MEM <= memoryPerSlave. Otherwise Spark will just hang.
        val memoryPerSlaveInt = memoryPerSlave.toInt
        val sparkMemEnv = System.getenv("SPARK_MEM")
        val sparkMemEnvInt = if (sparkMemEnv != null) Utils.memoryStringToMb(sparkMemEnv) else 512
        if (sparkMemEnvInt > memoryPerSlaveInt) {
          throw new SparkException(
            "Slave memory (%d MB) cannot be smaller than SPARK_MEM (%d MB)".format(
              memoryPerSlaveInt, sparkMemEnvInt))
        }

        val scheduler = new ClusterScheduler(this)
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt)
        val sparkUrl = localCluster.start()
        val backend = new SparkDeploySchedulerBackend(scheduler, this, sparkUrl, jobName)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: SparkDeploySchedulerBackend) => {
          localCluster.stop()
        }
        scheduler

      case _ =>
        MesosNativeLibrary.load()
        val scheduler = new ClusterScheduler(this)
        val coarseGrained = System.getProperty("spark.mesos.coarse", "false").toBoolean
        val backend = if (coarseGrained) {
          new CoarseMesosSchedulerBackend(scheduler, this, master, jobName)
        } else {
          new MesosSchedulerBackend(scheduler, this, master, jobName)
        }
        scheduler.initialize(backend)
        scheduler
    }
  }
  taskScheduler.start()
  
  
  def defaultParallelism: Int = taskScheduler.defaultParallelism
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }
}

object SparkContext {
} 