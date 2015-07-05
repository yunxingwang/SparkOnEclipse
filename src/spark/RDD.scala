package spark
import scala.reflect.ClassTag
/**
 * @author guotong
 */
class RDD [T: ClassTag](@transient sc: SparkContext)extends Serializable{
  
}