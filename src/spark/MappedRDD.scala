package spark

import spark.OneToOneDependency
import spark.RDD
import spark.Split
import scala.reflect.ClassTag

private[spark]
class MappedRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: T => U)
  extends RDD[U](prev.context) {
  
  override def splits = prev.splits
  val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = prev.iterator(split).map(f)
}