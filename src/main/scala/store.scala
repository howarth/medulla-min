package main.scala.store

import java.nio.file.{Path, Paths, Files}
import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer}
import java.nio.{ByteBuffer, DoubleBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.Executors

import main.scala.core._
import main.java.constants.DataTypeStrings

import scala.collection.JavaConverters._

import breeze.linalg.{DenseMatrix, DenseVector}


class AlphaContext(
                    val dataRegistry : DataRegistry,
                    val dataStore : DataStore with
                      ScalarRWDataStore with
                      VectorRWDataStore with
                      Matrix2DRWDataStore with
                      SingleChannelTimeSeriesRWDataStore with
                      MultiChannelTimeSeriesRWDataStore
                  ) extends DataContext
{

  def put(id : DataId, data : Data) : Unit = {
    dataRegistry.registerData(id, data.metadata)
    (id, data) match {
      case (scalarId : ScalarId, scalarData : ScalarData[_]) => dataStore.putScalar(scalarId, scalarData)
      case (vectorId : VectorId, vectorData : VectorData[_]) => dataStore.putVector(vectorId, vectorData)
      case (matrixId: Matrix2DId, matrixData: Matrix2DData[_]) => dataStore.putMatrix2D(matrixId, matrixData)
      case (tsId: SingleChannelTimeSeriesId, timeSeriesData: SingleChannelTimeSeriesData[_]) =>
        dataStore.putSingleChannelTimeSeries(tsId, timeSeriesData)
      case (tsId: MultiChannelTimeSeriesId, timeSeriesData: MultiChannelTimeSeriesData[_]) =>
        dataStore.putMultiChannelTimeSeries(tsId, timeSeriesData)
      case _ => throw new Exception(s"Unknown DataId type and DataType $id.id")
    }
  }

  def get(id : DataId): Data ={
    if(!dataRegistry.dataIsRegistered(id)){
      throw new Exception(s"Data does not exist for DataId $id")
    }
    id match {
      case (scalarId: ScalarId) => dataStore.getScalar(scalarId)
      case (vectorId: VectorId) => dataStore.getVector(vectorId)
      case (matrixId: Matrix2DId) => dataStore.getMatrix2D(matrixId)
      case (tsId: SingleChannelTimeSeriesId) => dataStore.getSingleChannelTimeSeries(tsId)
      case (tsId: MultiChannelTimeSeriesId) => dataStore.getMultiChannelTimeSeries(tsId)
      /*
    case (matrixId: Matrix2DId) => dataStore.getMatrix2D(matrixId)
      */
      case _ => throw new Exception("Unknown DataId")
    }

  }

  /*
  def getFromString(s : String) : Data = get(DoubleScalarId(s))

  def putFromStringAndJavaParamMap(identifier : String, paramMap : java.util.Map[String, Any]) : Unit = {
    println(paramMap)
    val pMap : Map[String,Any] = paramMap.asScala.toMap
    pMap(DataTypeStrings.TYPE_STR_KEY)  match {
      case DataTypeStrings.DOUBLE_SCALAR =>c
        this.put(DoubleScalarId(identifier),
          DoubleScalarData(pMap(DataTypeStrings.SINGLE_VALUE_KEY).asInstanceOf[Double]))
      case _ => throw new Error()
    }

  }
  */

  def delete(id : DataId) : Unit = {
    dataRegistry.deleteData(id)
    id match {
      case (scalarId: ScalarId) => dataStore.deleteScalar(scalarId)
      /*
    case (vectorId: VectorId) => dataStore.deleteVector(vectorId)
    case (matrixId: Matrix2DId) => dataStore.deleteMatrix2D(matrixId)
    case (tsId: SingleChannelTimeSeriesId) => dataStore.deleteSingleChannelTimeSeries(tsId)
    case (tsId: MultiChannelTimeSeriesId) =>
      dataStore.deleteMultiChannelTimeSeries(tsId)
      */
      case _ => throw new Exception("Unknown DataId type")
    }
  }
}

class BinStore(baseDirectoryString : String, nThreads : Int) extends DataStore with
  ScalarRWDataStore with
  VectorRWDataStore with
  Matrix2DRWDataStore with
  MultiChannelTimeSeriesRWDataStore{


  val channelsFileKey : String = "channels"
  val timesFileKey : String = "times"
  val baseDirPath : Path = Paths.get(baseDirectoryString)
  val mctsBasePathFn : MultiChannelTimeSeriesId => Path =
    (id) => id.ancestors.map(_.toString).foldLeft(baseDirPath)((p1 : Path, p2 : String) => p1.resolve(p2))
  def channelPathFn(id : MultiChannelTimeSeriesId, channelName : String) : Path = {
    val fileSep = id.fileSep
    mctsBasePathFn(id).resolve(id.ancestors.mkString(fileSep).concat(s"$fileSep$channelName.bin"))
  }

  override def putMatrix2D[T](id: Matrix2DId, data: Matrix2DData[T]): Unit = ???

  override def deleteMatrix2D(id: Matrix2DId): Unit = ???

  override def getScalar(id: ScalarId): ScalarData[_] = ???

  override def putVector[T](id: VectorId, data: VectorData[T]): Unit = ???

  override def deleteVector(id: VectorId): Unit = ???

  override def putMultiChannelTimeSeries(id: MultiChannelTimeSeriesId, data: MultiChannelTimeSeriesData[_]): Unit = ???

  override def deleteMultiChannelTimeSeries(id: MultiChannelTimeSeriesId): Unit = ???

  override def putScalar[T](id: ScalarId, data: ScalarData[T]): Unit = ???

  override def deleteScalar(id: ScalarId): Unit = ???

  def readFullChannel(id : MultiChannelTimeSeriesId, channelId: TimeSeriesChannelId, array : Array[Double], start : Int, length : Int) : Unit = {
    val channelFile : File = channelPathFn(id, channelId.toString).toFile
    val raf = new RandomAccessFile(channelFile, "r")
    val inChannel = raf.getChannel
    val mbb : MappedByteBuffer= inChannel.map(FileChannel.MapMode.READ_ONLY, 0, channelFile.length())
    val sfreq : BigDecimal = BigDecimal(mbb.getDouble)
    val nSamples : Long = mbb.getLong
    val mdb : DoubleBuffer = mbb.asDoubleBuffer
    mdb.get(array, start, length)
    mbb.clear()
    mdb.clear()
    raf.close()
    inChannel.close()
  }

  override def getMultiChannelTimeSeries(id: MultiChannelTimeSeriesId): MultiChannelTimeSeriesData[_] = {
    val channels : Vector[TimeSeriesChannelId] = this.getChannels(id)
    val metadata : MultiChannelTimeSeriesMetadata[Double] = this.getMultiChannelTimeSeriesMetadata(id)
    id match {
      case id : DoubleMultiChannelTimeSeriesId =>{
        val dataArray = Array.ofDim[Double](metadata.length * channels.length)
        val futures = channels.zipWithIndex.map{case (chanId : TimeSeriesChannelId, i : Int) => {
          implicit val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)
          Future {
            readFullChannel(id, chanId, dataArray, i * metadata.length, metadata.length)
          }
        }
        }
        val data : DenseMatrix[Double] = new DenseMatrix[Double](metadata.length, channels.length, dataArray)
        Await.ready(Future.sequence(futures), Duration.Inf)
        DoubleMultiChannelTimeSeriesData(data, null, channels)
      }

      case _ => throw new NotImplementedError("Type of timeseries doesn't exist")
    }
  }

  def getMultiChannelTimeSeriesMetadata(id : MultiChannelTimeSeriesId) : MultiChannelTimeSeriesMetadata[Double] = {
    val timeMetadata = Files.readAllLines(channelPathFn(id, timesFileKey)).asScala.toVector
    new MultiChannelTimeSeriesMetadata[Double](timeMetadata(1).toInt, BigDecimal(timeMetadata(0)))
  }

  def getChannels(id: MultiChannelTimeSeriesId, group : String = "meg"): Vector[TimeSeriesChannelId] = {
    val chansPath : Path = channelPathFn(id, channelsFileKey)
    val chans = Files.readAllLines(chansPath).asScala.toVector.map(TimeSeriesChannelId(_))
    group match {
      case "all" => chans
      case "meg" => chans.slice(0,306)
    }

  }

  override def getVector(id: VectorId): VectorData[_] = ???

  override def getMatrix2D(id: Matrix2DId): Matrix2DData[_] = ???
}

