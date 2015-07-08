package spark.storage


import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream

import java.io.{InputStream, OutputStream, Externalizable, ObjectInput, ObjectOutput}
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.collection.JavaConversions._

import spark.serializer.Serializer
import spark.util.ByteBufferInputStream
import sun.nio.ch.DirectBuffer


private[spark] class BlockManagerId(var ip: String, var port: Int) extends Externalizable {
  def this() = this(null, 0)  // For deserialization only

  def this(in: ObjectInput) = this(in.readUTF(), in.readInt())

  override def writeExternal(out: ObjectOutput) {
    out.writeUTF(ip)
    out.writeInt(port)
  }

  override def readExternal(in: ObjectInput) {
    ip = in.readUTF()
    port = in.readInt()
  }

  override def toString = "BlockManagerId(" + ip + ", " + port + ")"

  override def hashCode = ip.hashCode * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId => port == id.port && ip == id.ip
    case _ => false
  }
}

