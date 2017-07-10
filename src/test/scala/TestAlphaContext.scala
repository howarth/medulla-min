import org.scalatest._
import main.scala.core._
import main.scala.store._
import scala.util.Random
import breeze.linalg.{DenseMatrix, DenseVector}

/**
  * Created by dhowarth on 7/10/17.
  */
class TestAlphaContext extends FlatSpec with Matchers{

  val binStore = new BinStore("/Users/dhowarth/work/db/bindata/")
  val binRegistry = new BinRegistry("/Users/dhowarth/work/db/bindata/")
  val alphaContext = new AlphaContext(binRegistry, binStore)
  val r = new Random(0)

  val doubleMatrices : List[DenseMatrix[Double]] = Range(0,10).map(_ => (r.nextInt(40)+2,r.nextInt(40)))
    .map(dims => new DenseMatrix(dims._1, dims._2, Array.fill(dims._1*dims._2)(r.nextDouble))).toList
  val intMatrices : List[DenseMatrix[Int]] = Range(0,10).map(_ => (r.nextInt(40)+2,r.nextInt(40)))
    .map(dims => new DenseMatrix(dims._1, dims._2, Array.fill(dims._1*dims._2)(r.nextInt))).toList
  val booleanMatrices : List[DenseMatrix[Boolean]] = Range(0,10).map(_ => (r.nextInt(40)+2,r.nextInt(40)))
    .map(dims => new DenseMatrix(dims._1, dims._2, Array.fill(dims._1*dims._2)(r.nextBoolean))).toList



  "putting double MCTS" should "put and get correctly" in {
    for ((tm, i) <- doubleMatrices.zipWithIndex) {
      val ts = DoubleMultiChannelTimeSeriesData(tm,
        Range(0, tm.rows).map(_ * BigDecimal(.001)).map(Timestamp(_)).toVector,
        Range(0, tm.cols).map(ci => ci.toString).map(cs => TimeSeriesChannelId(cs)).toVector
      )
      val iStr = i.toString
      val testId = DoubleMultiChannelTimeSeriesId(s"test:doubleMCTS$iStr")
      alphaContext.put(testId, ts)

      val d = alphaContext.get(testId)
      d.asInstanceOf[MultiChannelTimeSeriesData[Double]] should be(ts)
    }
  }


}
