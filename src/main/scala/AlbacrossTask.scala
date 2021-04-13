
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object AlbacrossTask {

  val MAX_IP: Long = 4294967295L
  val Threshold: Long = (MAX_IP / 4)

  private lazy val sparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("IntersectionFilter")
    .getOrCreate()

  private lazy val sc = sparkSession.sparkContext
  private lazy val data: DataFrame = sparkSession.read.option("header", "true").csv("src/main/resources/ipRanges.csv")

  private lazy val rangesRDD: RDD[(Long, Long)] = {
    data
      .select("start", "end")
      .rdd
      .map(r => { // map to pair (start: String, end: String)
        val startRangeString: String = r.getAs[String](0)
        val endRangeString: String = r.getAs[String](1)
        (startRangeString, endRangeString)
      })
      .map { // map string values of IP to Long values
        case (start, end) => (IPUtils.ipv4ToLong(start), IPUtils.ipv4ToLong(end))
      }

  }
  val rdd1 = rangesRDD.filter{case(_, end) => end <= Threshold}
  val rdd2 = rangesRDD.filter{case(_, end) => end > Threshold && end <= Threshold * 2}
  val rdd3 = rangesRDD.filter{case(_, end) => end > 2 * Threshold && end <= Threshold * 3}
  val rdd4 = rangesRDD.filter{case(_, end) => end > 3 * Threshold && end <= MAX_IP}

  val buckets1: RDD[(Long, Long)] = sc // RDD[Index, Occurrences]
      .parallelize(List.range(0, Threshold).map(idx => (idx, 0))) // IP mapped to numeric values is always between 0 and IP_MAX(=4294967295), so I create buckets for these values
      .map { case (index, _) => (index, existInIntervals(index, rdd1)) } // get number of intervals overlapping bucket (intervals which contain this adress IP)

  lazy val buckets2: RDD[(Long, Long)] = sc
    .parallelize(List.range(Threshold + 1,  2 * Threshold +1).map(idx => (idx, 0)))
    .map { case (index, _) => (index, existInIntervals(index, rdd2)) }

  lazy val buckets3: RDD[(Long, Long)] = sc
    .parallelize(List.range(2 * Threshold + 1, 3 * Threshold +1).map(idx => (idx, 0)))
    .map { case (index, _) => (index, existInIntervals(index, rdd3)) }

  lazy val buckets4: RDD[(Long, Long)] = sc
    .parallelize(List.range(3 * Threshold, MAX_IP +1).map(idx => (idx, 0)))
    .map { case (index, _) => (index, existInIntervals(index, rdd4)) }


  def existInIntervals(index: Long, intervals: RDD[(Long, Long)]) = {
      val newValue = intervals.filter { case (start, _) => start <= index }.map(el => 1).count()
      newValue
    }

    def printResults(buckets: RDD[(Long, Long)]) = {
      val ips = buckets.collect {
        case (index, occurences) if occurences == 1 => index
      }.toLocalIterator.toList

      val intervals = getIntervals(ips)
      val stringValue = intervals.map(l => (l.min, l.max)).mkString(",")
      println(stringValue)

    }


  def getIntervals(ips: List[Long]) = ips.foldLeft[(List[List[Long]], List[Long])]((Nil, Nil)) {
    case ((intervals, currentInterval), ip) => currentInterval match {
      case lastIP :: _ :: _ => if(ip - lastIP == 1) (intervals, ip :: currentInterval) else (List(ip, lastIP) :: (currentInterval :: intervals), List(ip))
      case lastIP :: Nil => (intervals, ip :: currentInterval)
      case Nil => (intervals, List(ip))
    }
  }._1.reverse.map(_.reverse)



  def main(args: Array[String]): Unit = {
    printResults(buckets1)
//    printResults(buckets2)
//    printResults(buckets3)
//    printResults(buckets4)
  }
}
