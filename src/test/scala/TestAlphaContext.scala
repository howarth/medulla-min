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

  val doubleScalars : Array[Double] = Array.fill(100)(r.nextDouble)
  val intScalars : Array[Int] = Array.fill(100)(r.nextInt)
  val booleanScalars : Array[Boolean] = Array.fill(100)(r.nextBoolean)

  val doubleVectors : List[DenseVector[Double]] = Range(0,10).map(_ => r.nextInt(40)+2)
    .map(dim => new DenseVector(Array.fill(dim)(r.nextDouble))).toList
  val intVectors : List[DenseVector[Int]] = Range(0,10).map(_ => r.nextInt(40)+2)
    .map(dim => new DenseVector(Array.fill(dim)(r.nextInt))).toList
  val booleanVectors : List[DenseVector[Boolean]] = Range(0,10).map(_ => r.nextInt(40)+2)
    .map(dim => new DenseVector(Array.fill(dim)(r.nextBoolean))).toList

  val doubleMatrices : List[DenseMatrix[Double]] = Range(0,10).map(_ => (r.nextInt(40)+2,r.nextInt(40)))
    .map(dims => new DenseMatrix(dims._1, dims._2, Array.fill(dims._1*dims._2)(r.nextDouble))).toList
  val intMatrices : List[DenseMatrix[Int]] = Range(0,10).map(_ => (r.nextInt(40)+2,r.nextInt(40)))
    .map(dims => new DenseMatrix(dims._1, dims._2, Array.fill(dims._1*dims._2)(r.nextInt))).toList
  val booleanMatrices : List[DenseMatrix[Boolean]] = Range(0,10).map(_ => (r.nextInt(40)+2,r.nextInt(40)))
    .map(dims => new DenseMatrix(dims._1, dims._2, Array.fill(dims._1*dims._2)(r.nextBoolean))).toList

  "puting double scalars" should "put and get correctly" in {
    for ((ts, i) <- doubleScalars.zipWithIndex) {
      val vec : DoubleScalarData = DoubleScalarData(ts)
      val iStr = i.toString
      val testId = DoubleScalarId(s"test:doubleScalar$iStr")
      alphaContext.put(testId, vec)
      val d = alphaContext.get(testId).asInstanceOf[DoubleScalarData]
      d should be (vec)
    }
  }

  "puting double vectors" should "put and get correctly" in {
    for ((tv, i) <- doubleVectors.zipWithIndex) {
      val vec : DoubleVectorData = DoubleVectorData(tv)
      val iStr = i.toString
      val testId = DoubleVectorId(s"test:doubleVector$iStr")
      alphaContext.put(testId, vec)
      val d = alphaContext.get(testId).asInstanceOf[DoubleVectorData]
      d should be (vec)
    }
  }

  "puting int vectors" should "put and get correctly" in {
    for ((tv, i) <- intVectors.zipWithIndex) {
      val vec : IntVectorData = IntVectorData(tv)
      val iStr = i.toString
      val testId = IntVectorId(s"test:intVector$iStr")
      alphaContext.put(testId, vec)
      val d = alphaContext.get(testId).asInstanceOf[IntVectorData]
      d should be (vec)
    }
  }

  "puting double matrices" should "put and get correctly" in {
    for ((tv, i) <- doubleMatrices.zipWithIndex) {
      val vec : DoubleMatrix2DData = DoubleMatrix2DData(tv)
      val iStr = i.toString
      val testId = DoubleMatrix2DId(s"test:doubleMatrix$iStr")
      alphaContext.put(testId, vec)
      val d = alphaContext.get(testId).asInstanceOf[DoubleMatrix2DData]
      d should be (vec)
    }
  }

  "puting int matricies" should "put and get correctly" in {
    for ((tv, i) <- intMatrices.zipWithIndex) {
      val vec : IntMatrix2DData = IntMatrix2DData(tv)
      val iStr = i.toString
      val testId = IntMatrix2DId(s"test:intMatrix$iStr")
      alphaContext.put(testId, vec)
      val d = alphaContext.get(testId).asInstanceOf[IntMatrix2DData]
      d should be (vec)
    }
  }
  /**************************************
   MCTS
   */

  "putting double MCTS" should "put and get correctly" in {
    for ((tm, i) <- doubleMatrices.zipWithIndex) {
      val ts = DoubleMultiChannelTimeSeriesData(tm,
        Range(0, tm.rows).map(_ * BigDecimal(.001)).map(Timestamp(_)).toVector,
        Range(0, tm.cols).map(ci => ci.toString).map(cs => TimeSeriesChannelId(cs)).toVector
      )
      val iStr = i.toString
      val testId = DoubleMultiChannelTimeSeriesId(s"test:doubleMCTS$iStr")
      alphaContext.put(testId, ts)

      val d = alphaContext.get(testId).asInstanceOf[DoubleMultiChannelTimeSeriesData]
      d should be(ts)
    }
  }

  "putting int MCTS" should "put and get correctly" in {
    for ((tm, i) <- intMatrices.zipWithIndex) {
      val ts = IntMultiChannelTimeSeriesData(tm,
        Range(0, tm.rows).map(_ * BigDecimal(.001)).map(Timestamp(_)).toVector,
        Range(0, tm.cols).map(ci => ci.toString).map(cs => TimeSeriesChannelId(cs)).toVector
      )
      val iStr = i.toString
      val testId = IntMultiChannelTimeSeriesId(s"test:intMCTS$iStr")
      alphaContext.put(testId, ts)

      val d = alphaContext.get(testId).asInstanceOf[IntMultiChannelTimeSeriesData]
      d should be(ts)
    }
  }

  "putting boolean MCTS" should "put and get correctly" in {
    for ((tm, i) <- booleanMatrices.zipWithIndex) {
      val ts = BooleanMultiChannelTimeSeriesData(tm,
        Range(0, tm.rows).map(_ * BigDecimal(.001)).map(Timestamp(_)).toVector,
        Range(0, tm.cols).map(ci => ci.toString).map(cs => TimeSeriesChannelId(cs)).toVector
      )
      val iStr = i.toString
      val testId = BooleanMultiChannelTimeSeriesId(s"test:booleanMCTS$iStr")
      alphaContext.put(testId, ts)

      val d = alphaContext.get(testId).asInstanceOf[BooleanMultiChannelTimeSeriesData]
      d should be(ts)
    }
  }


  "getting random windows of double MCTS" should " get correctly" in {

  }

}
