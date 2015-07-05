package spark
import scala.reflect.ClassTag
/**
 * @author guotong
 */
class SparkContext(master: String, frameworkName: String) {
  def defaultParallelism: Int = taskScheduler.defaultParallelism
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }
}

object SparkContext {
} 