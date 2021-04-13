import spire.algebra
import spire.algebra.Order
import spire.math.extras.interval.IntervalTrie
import spire.std.any.LongAlgebra



object IPUtils {
  def ipv4ToLong(ip: String): Long =
    ip.split('.').ensuring(_.length == 4)
      .map(_.toLong).ensuring(_.forall(x => x >= 0 && x < 256))
      .reverse.zip(List(0,8,16,24)).map(xi => xi._1 << xi._2).sum

  def longToipv4(ip: Long): String =
    List(0x000000ff, 0x0000ff00, 0x00ff0000, 0xff000000).zip(List(0,8,16,24))
      .map(mi => ((mi._1 & ip) >> mi._2)).reverse
      .map(_.toString).mkString(".")
}


