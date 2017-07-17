package main.scala.core

import scala.collection.breakOut
import scala.reflect.runtime.universe._
import main.java.constants.DataTypeStrings
import breeze.linalg.{DenseVector, DenseMatrix}

object DataTypeStringData{
  val typeStringFromData : (Data => String) = (d : Data) => d match {
    case _ : DoubleScalarData => DataTypeStrings.DOUBLE_SCALAR
    case _ : IntScalarData => DataTypeStrings.INT_SCALAR
    case _ : BooleanScalarData => DataTypeStrings.BOOLEAN_SCALAR
    case _ => throw new NotImplementedError()
  }
  def typeStringFromId(id : DataId) : String = id match {
    case _ : DoubleScalarId => DataTypeStrings.DOUBLE_SCALAR
    case _ : IntScalarId => DataTypeStrings.INT_SCALAR
    case _ : BooleanScalarId => DataTypeStrings.BOOLEAN_SCALAR
    case _ : DoubleVectorId => DataTypeStrings.DOUBLE_VECTOR
    case _ : IntVectorId => DataTypeStrings.INT_VECTOR
    case _ : BooleanVectorId => DataTypeStrings.BOOLEAN_VECTOR
    case _ : DoubleMatrix2DId => DataTypeStrings.DOUBLE_MATRIX
    case _ : IntMatrix2DId => DataTypeStrings.INT_MATRIX
    case _ : BooleanMatrix2DId => DataTypeStrings.BOOLEAN_MATRIX
    case _ : DoubleMultiChannelTimeSeriesId => DataTypeStrings.DOUBLE_TS
    case _ : IntMultiChannelTimeSeriesId => DataTypeStrings.INT_TS
    case _ : BooleanMultiChannelTimeSeriesId => DataTypeStrings.BOOLEAN_TS
    case _ => throw new NotImplementedError()
  }
  def idFromTypeStringAndIdString(typeString : String, idStr : String) : DataId = {
    typeString match {
      case DataTypeStrings.DOUBLE_SCALAR => DoubleScalarId(idStr)
      case DataTypeStrings.INT_SCALAR => IntScalarId(idStr)
      case DataTypeStrings.BOOLEAN_SCALAR => BooleanScalarId(idStr)
      case DataTypeStrings.DOUBLE_VECTOR => DoubleVectorId(idStr)
      case DataTypeStrings.INT_VECTOR => IntVectorId(idStr)
      case DataTypeStrings.BOOLEAN_VECTOR => BooleanVectorId(idStr)
      case DataTypeStrings.DOUBLE_MATRIX => DoubleMatrix2DId(idStr)
      case DataTypeStrings.INT_MATRIX => IntMatrix2DId(idStr)
      case DataTypeStrings.BOOLEAN_MATRIX  => BooleanMatrix2DId(idStr)
      case DataTypeStrings.DOUBLE_TS => DoubleMultiChannelTimeSeriesId(idStr)
      case DataTypeStrings.INT_TS => IntMultiChannelTimeSeriesId(idStr)
      case DataTypeStrings.BOOLEAN_TS  => BooleanMultiChannelTimeSeriesId(idStr)
    }
  }
}

abstract class Data {
  val data : Any // Todo make a general type?
  val metadata : Metadata

  override def equals(that: Any): Boolean = {
    this.getClass == that.getClass && that.asInstanceOf[Data].data == this.data
  }
}
trait Metadata {
}

class HomogenousDataMetadata[T](implicit tag : TypeTag[T]) extends Metadata {
  val dataTypeTag : TypeTag[T] = tag
}

class ScalarMetadata[T](implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)
class ScalarData[T](val data : T)(implicit tag : TypeTag[T]) extends Data {
  val metadata = new ScalarMetadata[T]()
}

class DoubleScalarData(data : Double) extends ScalarData[Double](data)
class IntScalarData(data : Int) extends ScalarData[Int](data)
class BooleanScalarData(data : Boolean) extends ScalarData[Boolean](data)
object DoubleScalarData { def apply(v : Double) = new DoubleScalarData(v)}
object IntScalarData { def apply(v : Int) = new IntScalarData(v)}
object BooleanScalarData { def apply(v : Boolean) = new BooleanScalarData(v)}


class VectorData[T](val data : DenseVector[T])(implicit tag : TypeTag[T]) extends Data {
  val metadata = new VectorMetadata[T](data.length)(tag)
}
class VectorMetadata[T](val length : Int)(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleVectorData(data : DenseVector[Double]) extends VectorData[Double](data)
class IntVectorData(data : DenseVector[Int]) extends VectorData[Int](data)
class BooleanVectorData(data : DenseVector[Boolean]) extends VectorData[Boolean](data)
object DoubleVectorData {def apply(v : DenseVector[Double]) = new DoubleVectorData(v)}
object IntVectorData {def apply(v : DenseVector[Int]) = new IntVectorData(v)}
object BooleanVectorData {def apply(v : DenseVector[Boolean]) = new BooleanVectorData(v)}

/*
  Matrix 2D
 */
class Matrix2DData[T](val data : DenseMatrix[T])(implicit tag : TypeTag[T]) extends Data {
  val metadata = new Matrix2DMetadata[T](data.rows, data.cols)(tag)
}
class Matrix2DMetadata[T](val rows : Int, val cols : Int)(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleMatrix2DData(data : DenseMatrix[Double]) extends Matrix2DData[Double](data)
class IntMatrix2DData(data : DenseMatrix[Int]) extends Matrix2DData[Int](data)
class BooleanMatrix2DData(data : DenseMatrix[Boolean]) extends Matrix2DData[Boolean](data)

object DoubleMatrix2DData {def apply(v : DenseMatrix[Double]) = new DoubleMatrix2DData(v)}
object IntMatrix2DData {def apply(v : DenseMatrix[Int]) = new IntMatrix2DData(v)}
object BooleanMatrix2DData {def apply(v : DenseMatrix[Boolean]) = new BooleanMatrix2DData(v)}


abstract class SingleChannelTimeSeriesData[T](
    val data : Vector[T],
    val times : Vector[Timestamp],
    val channel : TimeSeriesChannelId)
  extends Data {
  val metadata = new SingleChannelTimeSeriesMetadata(times.length)

  override def equals(that: Any): Boolean = {
    if(this.getClass != that.getClass) return false
    val thatCast = that.asInstanceOf[SingleChannelTimeSeriesData[_]]
    this.times == thatCast.times && thatCast.channel == this.channel &&  thatCast.data == this.data
  }
}
class SingleChannelTimeSeriesMetadata[T](val length : Int)(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleSingleChannelTimeSeriesData( data : Vector[Double],
    times : Vector[Timestamp],
    channel : TimeSeriesChannelId) extends SingleChannelTimeSeriesData[Double](data, times, channel)
class IntSingleChannelTimeSeriesData( data : Vector[Int],
                                         times : Vector[Timestamp],
                                         channel : TimeSeriesChannelId) extends SingleChannelTimeSeriesData[Int](data, times, channel)
class BooleanSingleChannelTimeSeriesData( data : Vector[Boolean],
                                         times : Vector[Timestamp],
                                         channel : TimeSeriesChannelId) extends SingleChannelTimeSeriesData[Boolean](data, times, channel)

object DoubleSingleChannelTimeSeriesData {
  def apply(data : Vector[Double], times : Vector[Timestamp], channel : TimeSeriesChannelId) =
    new DoubleSingleChannelTimeSeriesData(data, times, channel)
}
object IntSingleChannelTimeSeriesData {
  def apply(data : Vector[Int], times : Vector[Timestamp], channel : TimeSeriesChannelId) =
    new IntSingleChannelTimeSeriesData(data, times, channel)
}
object BooleanSingleChannelTimeSeriesData {
  def apply(data : Vector[Boolean], times : Vector[Timestamp], channel : TimeSeriesChannelId) =
    new BooleanSingleChannelTimeSeriesData(data, times, channel)
}


abstract class MultiChannelTimeSeriesData[T](
    val data : DenseMatrix[T],
    val times : Vector[Timestamp],
    val channels : Vector[TimeSeriesChannelId])
  extends Data {
  val metadata = new MultiChannelTimeSeriesMetadata(times.length, BigDecimal(1)/(times(1).underlyingBD-times(0).underlyingBD))

  override def equals(that: Any): Boolean = {
    if(this.getClass != that.getClass) return false
    val thatCast = that.asInstanceOf[MultiChannelTimeSeriesData[_]]
    thatCast.times == this.times && thatCast.channels == this.channels &&  thatCast.data == this.data
  }
}
class MultiChannelTimeSeriesMetadata[T](val length : Int, val sfreq : BigDecimal)(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleMultiChannelTimeSeriesData( data : DenseMatrix[Double],
    times : Vector[Timestamp],
    channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesData[Double](data, times, channels)
class IntMultiChannelTimeSeriesData( data : DenseMatrix[Int],
                                         times : Vector[Timestamp],
                                         channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesData[Int](data, times, channels)
class BooleanMultiChannelTimeSeriesData( data : DenseMatrix[Boolean],
                                         times : Vector[Timestamp],
                                         channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesData[Boolean](data, times, channels)

object DoubleMultiChannelTimeSeriesData {
  def apply(data : DenseMatrix[Double], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new DoubleMultiChannelTimeSeriesData(data, times, channels)
}
object IntMultiChannelTimeSeriesData {
  def apply(data : DenseMatrix[Int], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new IntMultiChannelTimeSeriesData(data, times, channels)
}
object BooleanMultiChannelTimeSeriesData {
  def apply(data : DenseMatrix[Boolean], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new BooleanMultiChannelTimeSeriesData(data, times, channels)
}

class MultiChannelTimeSeriesWindowData[T](override val data : DenseMatrix[T],
                                          override val times : Vector[Timestamp],
                                          override val channels : Vector[TimeSeriesChannelId])
  extends MultiChannelTimeSeriesData[T](data, times, channels)


class DoubleMultiChannelTimeSeriesWindowData( data : DenseMatrix[Double],
                                        times : Vector[Timestamp],
                                        channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesWindowData[Double](data, times, channels)
class IntMultiChannelTimeSeriesWindowData( data : DenseMatrix[Int],
                                     times : Vector[Timestamp],
                                     channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesWindowData[Int](data, times, channels)
class BooleanMultiChannelTimeSeriesWindowData( data : DenseMatrix[Boolean],
                                         times : Vector[Timestamp],
                                         channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesWindowData[Boolean](data, times, channels)
object DoubleMultiChannelTimeSeriesWindowData {
  def apply(data : DenseMatrix[Double], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new DoubleMultiChannelTimeSeriesWindowData(data, times, channels)
}
object IntMultiChannelTimeSeriesWindowData {
  def apply(data : DenseMatrix[Int], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new IntMultiChannelTimeSeriesWindowData(data, times, channels)
}
object BooleanMultiChannelTimeSeriesWindowData {
  def apply(data : DenseMatrix[Boolean], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new BooleanMultiChannelTimeSeriesWindowData(data, times, channels)
}


/*
abstract class MultiChannelTimeSeriesData[T](
  val data : DenseMatrix[T]],
  val times : Vector[Timestamp],
  val channels : Vector[TimeSeriesChannelId]) extends Data{

  val channelToIndex  : Map[TimeSeriesChannelId, Int] = channels.zip(Range(0,channels.length))(breakOut)
}



class DoubleMultiChannelTimeSeriesData(data : DenseMatrix[Double]],
    times : Vector[Timestamp],
    channels : Vector[TimeSeriesChannelId])
  extends MultiChannelTimeSeriesData[Double](data, times, channels){

  val metadata = DoubleMultiChannelTimeSeriesMetadata(times.length, data.length)

  def getSingleChannel(channel : TimeSeriesChannelId) : DoubleSingleChannelTimeSeriesData = {
    val ind : Int = channelToIndex.get(channel) match {
      case Some(ind) => ind
      case None => throw new Exception("Channel doesn't exist")
    }
    new DoubleSingleChannelTimeSeriesData(data(ind), times, channel)
  }
}
object DoubleMultiChannelTimeSeriesData {
  def apply(data : DenseMatrix[Double]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new DoubleMultiChannelTimeSeriesData(data, times, channels)
}
class MultiChannelTimeSeriesMetadata[T](val length : Int, val nChannels : Int) extends HomogenousDataMetadata[T]
class DoubleMultiChannelTimeSeriesMetadata(length : Int, nChannels : Int) extends MultiChannelTimeSeriesMetadata[Double](length, nChannels)
object DoubleMultiChannelTimeSeriesMetadata {
  def apply(length : Int, nChannels : Int) = new DoubleMultiChannelTimeSeriesMetadata(length, nChannels)
}

*/