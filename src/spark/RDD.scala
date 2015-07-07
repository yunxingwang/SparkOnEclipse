package spark
import scala.reflect.ClassTag
import spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
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
}