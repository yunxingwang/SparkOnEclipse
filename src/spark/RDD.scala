package spark

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import spark.rdd.SampledRDD
import spark.util.Utils
/**
 * @author guotong
 */
abstract class RDD[T: ClassTag](@transient sc: SparkContext) extends Serializable {
  val id = sc.newRddId()
  def context = sc
  private[spark] val origin = Utils.getSparkCallSite
  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD(this, sc.clean(f))
  def splits: Array[Split]
   @transient val dependencies: List[Dependency[_]]
  final def iterator(split: Split): Iterator[T] = {
    compute(split)
  }
  def compute(split: Split): Iterator[T]
  def reduce(f: (T, T) => T): T = {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val options = sc.runJob(this, reducePartition)
    val results = new ArrayBuffer[T]
    for (opt <- options; elem <- opt) {
      results += elem
    }
    if (results.size == 0) {
      throw new UnsupportedOperationException("empty collection")
    } else {
      return results.reduceLeft(cleanF)
    }
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = {
    sc.runJob(this, (iter: Iterator[T]) => {
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next
      }
      result
    }).sum
  }

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T] =
    new SampledRDD(this, withReplacement, fraction, seed)

  def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }
  def preferredLocations(split: Split): Seq[String] = Nil
}