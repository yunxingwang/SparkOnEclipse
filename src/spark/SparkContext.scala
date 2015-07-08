package spark
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap
import spark.scheduler.local.LocalScheduler
import scala.reflect.ClassTag
import spark.scheduler.TaskScheduler
import spark.scheduler.cluster._
import spark.util.Utils
import spark.scheduler._
import spark.deploy.LocalSparkCluster
/**
 */
class SparkContext(
  master: String,
  jobName: String,
  val sparkHome: String,
  jars: Seq[String],
  environment: Map[String, String])
    extends Logging {
  def this(master: String, jobName: String, sparkHome: String, jars: Seq[String]) =
    this(master, jobName, sparkHome, jars, Map())
  def this(master: String, jobName: String) = this(master, jobName, null, Nil, Map())
  if (System.getProperty("spark.master.host") == null) {
    System.setProperty("spark.master.host", Utils.localIpAddress())
  }
  if (System.getProperty("spark.master.port") == null) {
    System.setProperty("spark.master.port", "7075")
  }
  private var nextRddId = new AtomicInteger(0)
  private var nextShuffleId = new AtomicInteger(0)
  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()
  private[spark] val addedFiles = HashMap[String, Long]()
  private[spark] val addedJars = HashMap[String, Long]()
  private val isLocal = (master == "local" || master.startsWith("local["))
  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()
    // Environment variables to pass to our executors
  private[spark] val executorEnvs = HashMap[String, String]()
  for (key <- Seq("SPARK_MEM", "SPARK_CLASSPATH", "SPARK_LIBRARY_PATH", "SPARK_JAVA_OPTS",
       "SPARK_TESTING")) {
    val value = System.getenv(key)
    if (value != null) {
      executorEnvs(key) = value
    }
  }
  executorEnvs ++= environment
  
  // Create and start the scheduler
  private[spark] val env = SparkEnv.createFromSystemProperties(
    System.getProperty("spark.master.host"),
    System.getProperty("spark.master.port").toInt,
    true,
    isLocal)
  SparkEnv.set(env)
  private var taskScheduler: TaskScheduler = {
    // Regular expression used for local[N] master format
    val LOCAL_N_REGEX = """local\[([0-9]+)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """(spark://.*)""".r

    master match {
      case "local" =>
        new LocalScheduler(1, 0, this)

      case LOCAL_N_REGEX(threads) =>
        new LocalScheduler(threads.toInt, 0, this)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        new LocalScheduler(threads.toInt, maxFailures.toInt, this)

      case LOCAL_CLUSTER_REGEX() =>
        val numSlaves = 2
        val coresPerSlave = 1 
        val memoryPerSlave = 1024
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

    }
  }
  taskScheduler.start()
  private var dagScheduler = new DAGScheduler(taskScheduler)
  private[spark] def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }
  def defaultParallelism: Int = taskScheduler.defaultParallelism
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean): Array[U] = {
    val callSite = Utils.getSparkCallSite
    val result = dagScheduler.runJob(rdd, func, partitions, callSite, allowLocal)
    result
  }
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: Iterator[T] => U,
    partitions: Seq[Int],
    allowLocal: Boolean): Array[U] = {
    runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), partitions, allowLocal)
  }
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.splits.size, false)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.splits.size, false)
  }

}

object SparkContext {
} 