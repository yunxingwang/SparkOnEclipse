package spark

/**
 * @author guotong
 */
class SparkContext(master: String, frameworkName: String) {
  def defaultParallelism: Int = taskScheduler.defaultParallelism
  def parallelize[T: ClassManifest](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }
}

object SparkContext {
}