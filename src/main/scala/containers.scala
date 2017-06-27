package main.scala.core

import scala.collection.breakOut
import scala.reflect.runtime.universe._
import main.java.constants.DataTypeStrings

class DataTypeStringData(val typeString : String, val data : Data)
object DataTypeStringData{
  def apply(data : Data) = new DataTypeStringData(typeString(data), data)
  val typeString : (Data => String) = (d : Data) => d match {
    case _ : DoubleScalarData => DataTypeStrings.DOUBLE_SCALAR
    case _ : IntScalarData => DataTypeStrings.INT_SCALAR
    case _ : BooleanScalarData => DataTypeStrings.BOOLEAN_SCALAR
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


class VectorData[T](val data : Vector[T])(implicit tag : TypeTag[T]) extends Data {
  val metadata = new VectorMetadata[T](data.length)(tag)
}
class VectorMetadata[T](val length : Int)(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleVectorData(data : Vector[Double]) extends VectorData[Double](data)
class IntVectorData(data : Vector[Int]) extends VectorData[Int](data)
class BooleanVectorData(data : Vector[Boolean]) extends VectorData[Boolean](data)
object DoubleVectorData {def apply(v : Vector[Double]) = new DoubleVectorData(v)}
object IntVectorData {def apply(v : Vector[Int]) = new IntVectorData(v)}
object BooleanVectorData {def apply(v : Vector[Boolean]) = new BooleanVectorData(v)}

/*
  Matrix 2D
 */
class Matrix2DData[T](val data : Vector[Vector[T]])(implicit tag : TypeTag[T]) extends Data {
  val metadata = new Matrix2DMetadata[T](Tuple2(data.length, data(0).length))(tag)
}
class Matrix2DMetadata[T](val shape : Tuple2[Int,Int])(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleMatrix2DData(data : Vector[Vector[Double]]) extends Matrix2DData[Double](data)
class IntMatrix2DData(data : Vector[Vector[Int]]) extends Matrix2DData[Int](data)
class BooleanMatrix2DData(data : Vector[Vector[Boolean]]) extends Matrix2DData[Boolean](data)

object DoubleMatrix2DData {def apply(v : Vector[Vector[Double]]) = new DoubleMatrix2DData(v)}
object IntMatrix2DData {def apply(v : Vector[Vector[Int]]) = new IntMatrix2DData(v)}
object BooleanMatrix2DData {def apply(v : Vector[Vector[Boolean]]) = new BooleanMatrix2DData(v)}


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
    val data : Vector[Vector[T]],
    val times : Vector[Timestamp],
    val channels : Vector[TimeSeriesChannelId])
  extends Data {
  val metadata = new MultiChannelTimeSeriesMetadata(times.length)

  override def equals(that: Any): Boolean = {
    if(this.getClass != that.getClass) return false
    val thatCast = that.asInstanceOf[MultiChannelTimeSeriesData[_]]
    thatCast.times == this.times && thatCast.channels == this.channels &&  thatCast.data == this.data
  }
}
class MultiChannelTimeSeriesMetadata[T](val length : Int)(implicit tag : TypeTag[T]) extends HomogenousDataMetadata[T]()(tag)

class DoubleMultiChannelTimeSeriesData( data : Vector[Vector[Double]],
    times : Vector[Timestamp],
    channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesData[Double](data, times, channels)
class IntMultiChannelTimeSeriesData( data : Vector[Vector[Int]],
                                         times : Vector[Timestamp],
                                         channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesData[Int](data, times, channels)
class BooleanMultiChannelTimeSeriesData( data : Vector[Vector[Boolean]],
                                         times : Vector[Timestamp],
                                         channels : Vector[TimeSeriesChannelId]) extends MultiChannelTimeSeriesData[Boolean](data, times, channels)

object DoubleMultiChannelTimeSeriesData {
  def apply(data : Vector[Vector[Double]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new DoubleMultiChannelTimeSeriesData(data, times, channels)
}
object IntMultiChannelTimeSeriesData {
  def apply(data : Vector[Vector[Int]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new IntMultiChannelTimeSeriesData(data, times, channels)
}
object BooleanMultiChannelTimeSeriesData {
  def apply(data : Vector[Vector[Boolean]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new BooleanMultiChannelTimeSeriesData(data, times, channels)
}


/*
abstract class MultiChannelTimeSeriesData[T](
  val data : Vector[Vector[T]],
  val times : Vector[Timestamp],
  val channels : Vector[TimeSeriesChannelId]) extends Data{

  val channelToIndex  : Map[TimeSeriesChannelId, Int] = channels.zip(Range(0,channels.length))(breakOut)
}



class DoubleMultiChannelTimeSeriesData(data : Vector[Vector[Double]],
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
  def apply(data : Vector[Vector[Double]], times : Vector[Timestamp], channels : Vector[TimeSeriesChannelId]) =
    new DoubleMultiChannelTimeSeriesData(data, times, channels)
}
class MultiChannelTimeSeriesMetadata[T](val length : Int, val nChannels : Int) extends HomogenousDataMetadata[T]
class DoubleMultiChannelTimeSeriesMetadata(length : Int, nChannels : Int) extends MultiChannelTimeSeriesMetadata[Double](length, nChannels)
object DoubleMultiChannelTimeSeriesMetadata {
  def apply(length : Int, nChannels : Int) = new DoubleMultiChannelTimeSeriesMetadata(length, nChannels)
}

*/