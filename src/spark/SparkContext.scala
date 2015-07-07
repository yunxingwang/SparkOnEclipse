package spark
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap
import spark.scheduler.local.LocalScheduler
import scala.reflect.ClassTag
import spark.scheduler.TaskScheduler
import spark.util.Utils
import spark.scheduler._
/**
 * 先不接触文件中的数据，计算pi或者从内存生成数据</br>
 * 还是以0.6版本为主，1.4版本只是要里面的util
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

    }
  }
  taskScheduler.start()
 // private var dagScheduler = new DAGScheduler(taskScheduler)
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
   // val result = dagScheduler.runJob(rdd, func, partitions, callSite, allowLocal)
    null
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