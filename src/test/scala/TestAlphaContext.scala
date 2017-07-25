import org.scalatest._
import main.scala.core._
import main.scala.store._
import scala.util.Random
import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.contexts._

/**
  * Created by dhowarth on 7/10/17.
  */
class TestAlphaContext extends FlatSpec with Matchers{

  val alphaContext = EggsAlphaContext()
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

  "puting double scalars" should "put and get many correctly" in {
    val data : List[(Double, Int)] = doubleScalars.zipWithIndex.toList
    val dataAndId : List[(DoubleScalarId, DoubleScalarData)] =
      data.map(t =>{val id = t._1; (DoubleScalarId(s"many$id"), DoubleScalarData(t._2))})
    alphaContext.putMany(dataAndId)
    val justIds = dataAndId.map(_._1)
    val retData = alphaContext.getMany(justIds)
    val justData = dataAndId.map(_._2)
    retData should be (justData)
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
      val startTimeDouble = r.nextDouble()*r.nextInt(5) * (if(r.nextBoolean()) -1.0 else 1.0)
      val startTime = Timestamp(startTimeDouble)
      val sfreq = 1 + BigDecimal(r.nextDouble*r.nextInt(2000))
      val step = BigDecimal(1)/sfreq
      val times = Vector.tabulate[Timestamp](tm.rows){i : Int =>  Timestamp(startTime.underlyingBD + BigDecimal(i)/sfreq)}

      val ts = DoubleMultiChannelTimeSeriesData(tm,
        times,
        Range(0, tm.cols).map(ci => ci.toString).map(cs => TimeSeriesChannelId(cs)).toVector
      )
      val iStr = i.toString
      val testId = DoubleMultiChannelTimeSeriesId(s"test:doubleMCTS$iStr")
      alphaContext.put(testId, ts)

      val d = alphaContext.get(testId).asInstanceOf[DoubleMultiChannelTimeSeriesData]
      d should be(ts)

      for(i <- Range(0,10)){
        val lastI = times.length-1
        val startI = r.nextInt(times.length)
        val endI = startI + r.nextInt(times.length-startI)
        val subTimes = times.slice(startI, endI+1)
        val startT : Timestamp = startI match {
          case 0 => times(0)
          case i => if(r.nextBoolean) times(i) else Timestamp((times(i).underlyingBD+times(i-1).underlyingBD)/BigDecimal(2))
        }
        val endT : Timestamp = endI == lastI match {
          case true => { if(r.nextBoolean()) times(lastI) else Timestamp(times(lastI).underlyingBD+BigDecimal(1))}
          case false => {if(r.nextBoolean) times(endI) else Timestamp((times(endI).underlyingBD+times(endI+1).underlyingBD)/BigDecimal(2))}
        }
        println(testId, startT, endT)
        val windowId = DoubleMultiChannelTimeSeriesWindowId(testId, startT, endT)
        val subData = alphaContext.get(windowId).asInstanceOf[DoubleMultiChannelTimeSeriesWindowData]
        val returnedD = new DoubleMultiChannelTimeSeriesWindowData(tm(startI to endI,::), subTimes, d.channels)
        //subData.times should be (returnedD.times)
        subData.data should be (returnedD.data)
        //subData.channels should be (returnedD.channels)
        //subData should be (returnedD)
      }
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


}
