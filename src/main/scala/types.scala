package main.scala.store
import scala.reflect.runtime.universe._
import main.scala.core._
import main.java.constants.DataTypeStrings

case class DataIdNotInRegistryException(msg : String) extends Exception(msg)
case class DataIdNotInStoreException(msg : String) extends Exception(msg)
case class InconsistentDataIdTypeAndDataTypeException(msg : String) extends Exception(msg)

trait DataContext {
  val dataRegistry : DataRegistry
  val dataStore : DataStore
  def put(id : DataId, data : Data) : Unit
  def putMany(data : Seq[(DataId, Data)]) : Unit
  def get(id : DataId) : Data
  def getMany(ids : Seq[DataId]) : Vector[Data]
  def delete(id : DataId) : Unit
}


trait DataRegistry{
  def dataIsRegistered(id : DataId) : Boolean
  def registerData(id : DataId) : Unit
  def getTypedId(id : String) : DataId
}

trait DataStore {
  //def dataExists(id : DataId) : Boolean
}
trait ReadableDataStore extends DataStore
trait WritableDataStore extends DataStore

/* thoughts as I go here
should an ID have the type in it?
is there a way to do something like
trait ReadableDataStore {
  get[Data][T]
}
and then implement
get[Matrix2D][Double]
and then not write as much?
*/

/*
  Scalars
 */
trait ScalarStore
trait ScalarReadableDataStore extends ScalarStore with ReadableDataStore{
  def getScalar(id : ScalarId): ScalarData[_]
}
trait ScalarWritableDataStore extends ScalarStore with WritableDataStore{
  def putScalar(id : ScalarId, data : ScalarData[_])
  def deleteScalar(id : ScalarId) : Unit
}
trait ScalarRWDataStore extends ScalarReadableDataStore with ScalarWritableDataStore


/*
  Vectors
  */
trait VectorStore
trait VectorReadableDataStore extends VectorStore with ReadableDataStore{
  def getVector(id : VectorId): VectorData[_]
}
trait VectorWritableDataStore extends VectorStore with WritableDataStore{
  def putVector(id : VectorId, data : VectorData[_]) : Unit
  def deleteVector(id : VectorId) : Unit
}
trait VectorRWDataStore extends VectorReadableDataStore with VectorWritableDataStore

/*
  2D Matricies
 */
trait Matrix2DStore
trait Matrix2DReadableDataStore extends Matrix2DStore with ReadableDataStore{
  def getMatrix2D(id : Matrix2DId) : Matrix2DData[_]
}
trait Matrix2DWritableDataStore extends Matrix2DStore with WritableDataStore{
  def putMatrix2D(id : Matrix2DId, data : Matrix2DData[_]) : Unit
  def deleteMatrix2D(id : Matrix2DId) : Unit
}
trait Matrix2DRWDataStore extends Matrix2DWritableDataStore with Matrix2DReadableDataStore

/*
 TimeSeries
 */
trait TimeSeriesReadableDataStore extends ReadableDataStore {
  /*
  def getFirstTimestamp(id : TimeSeriesId) : Timestamp
  def getLastTimestamp(id : TimeSeriesId) : Timestamp
  def getTimes(id : TimeSeriesId) : Vector[Timestamp]
  */
}
trait TimeSeriesWritableDataStore extends WritableDataStore {
}
trait TimeSeriesRWDataStore extends TimeSeriesReadableDataStore with TimeSeriesWritableDataStore

trait SingleChannelTimeSeriesReadableDataStore extends TimeSeriesReadableDataStore{
  def getSingleChannelTimeSeries(id : SingleChannelTimeSeriesId): SingleChannelTimeSeriesData[_]
  def getChannel(id : SingleChannelTimeSeriesId) : TimeSeriesChannelId
}
trait SingleChannelTimeSeriesWritableDataStore extends TimeSeriesWritableDataStore{
  def putSingleChannelTimeSeries(id : SingleChannelTimeSeriesId, data : SingleChannelTimeSeriesData[_]) : Unit
  def deleteSingleChannelTimeSeries(id : SingleChannelTimeSeriesId) : Unit
}
trait SingleChannelTimeSeriesRWDataStore extends
  SingleChannelTimeSeriesReadableDataStore with
  SingleChannelTimeSeriesWritableDataStore

trait MultiChannelTimeSeriesReadableDataStore extends TimeSeriesReadableDataStore{
  def getMultiChannelTimeSeries(id : MultiChannelTimeSeriesId): MultiChannelTimeSeriesData[_]
  def getChannels(id : MultiChannelTimeSeriesId, group : String) : Vector[TimeSeriesChannelId]
}
trait MultiChannelTimeSeriesWritableDataStore extends TimeSeriesWritableDataStore{
  def putMultiChannelTimeSeries(id : MultiChannelTimeSeriesId, data : MultiChannelTimeSeriesData[_]) : Unit
  def deleteMultiChannelTimeSeries(id : MultiChannelTimeSeriesId) : Unit
}
trait MultiChannelTimeSeriesRWDataStore extends
  MultiChannelTimeSeriesReadableDataStore with
  MultiChannelTimeSeriesWritableDataStore


trait MultiChannelTimeSeriesWindowReadableDataStore extends ReadableDataStore {
  def getMultiChannelTimeSeriesWindow(id : MultiChannelTimeSeriesWindowId) : MultiChannelTimeSeriesWindowData[_]
}