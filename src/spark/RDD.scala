package spark
import scala.reflect.ClassTag
/**
 * @author guotong
 */
abstract class RDD [T: ClassTag](@transient sc: SparkContext)extends Serializable{
    val id = sc.newRddId()
}