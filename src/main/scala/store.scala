package main.scala.store

import java.nio.file.{Path, Paths, Files}
import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer}
import java.nio.{ByteBuffer, DoubleBuffer, MappedByteBuffer, IntBuffer}
import java.nio.channels.FileChannel
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.Executors
import java.nio.charset.StandardCharsets

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
                      MultiChannelTimeSeriesRWDataStore with
                      MultiChannelTimeSeriesWindowReadableDataStore
                  ) extends DataContext
{

  def put(id : DataId, data : Data) : Unit = {
    dataRegistry.registerData(id)
    (id, data) match {
      case (scalarId : ScalarId, scalarData : ScalarData[_]) => dataStore.putScalar(scalarId, scalarData)
      case (vectorId : VectorId, vectorData : VectorData[_]) => dataStore.putVector(vectorId, vectorData)
      case (matrixId: Matrix2DId, matrixData: Matrix2DData[_]) => dataStore.putMatrix2D(matrixId, matrixData)
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
      case (tsId: MultiChannelTimeSeriesId) => dataStore.getMultiChannelTimeSeries(tsId)
      case (tsId : MultiChannelTimeSeriesWindowId) => dataStore.getMultiChannelTimeSeriesWindow(tsId)
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
    /*
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
    */
  }
}

class BinRegistry(baseDirectoryString : String) extends DataRegistry{
  val registryChannelName = "registry"
  val storeUtils = new FileSystemStoreUtils(baseDirectoryString)

  def registryPath(id : DataId) : Path = storeUtils.channelPathFn(id, registryChannelName)

  override def dataIsRegistered(id: DataId): Boolean = {
    storeUtils.channelPathFn(id, registryChannelName).toFile.exists
  }
  override def registerData(id: DataId): Unit = {
    val p : Path = registryPath(id)
    storeUtils.dataBasePathFn(id).toFile.mkdirs()
    Files.write(p, DataTypeStringData.typeStringFromId(id).getBytes(StandardCharsets.UTF_8))
  }
  override def getTypedId(id: String): DataId = {
    val typeStr = new String(Files.readAllBytes(registryPath(new TypelessDataId(id))), StandardCharsets.UTF_8)
    DataTypeStringData.idFromTypeStringAndIdString(typeStr, id)
  }
}

class FileSystemStoreUtils(val baseDirString : String) {

  val baseDirPath = Paths.get(baseDirString)
  val dataBasePathFn : DataId => Path =
    (id) => id.ancestors.map(_.toString).foldLeft(baseDirPath)((p1 : Path, p2 : String) => p1.resolve(p2))
  def channelPathFn(id : DataId, channelName : String) : Path = {
    val fileSep = id.fileSep
    dataBasePathFn(id).resolve(id.ancestors.mkString(fileSep).concat(s"$fileSep$channelName.bin"))
  }
  def atomicDataPathFn(id : DataId) = dataBasePathFn(id).resolve(id.id)

}

class BinStore(baseDirectoryString : String, nThreads : Int = 32) extends DataStore with
  ScalarRWDataStore with
  VectorRWDataStore with
  Matrix2DRWDataStore with
  MultiChannelTimeSeriesRWDataStore with
  MultiChannelTimeSeriesWindowReadableDataStore {

  val storeUtils = new FileSystemStoreUtils(baseDirectoryString)
  val dataBasePathFn : MultiChannelTimeSeriesId => Path = storeUtils.dataBasePathFn
  def channelPathFn(id : MultiChannelTimeSeriesId, channelName : String) : Path = storeUtils.channelPathFn(id, channelName)

  val channelsFileKey : String = "channels"
  val timesFileKey : String = "times"

  override def getScalar(id: ScalarId): ScalarData[_] = {
    val file : File = storeUtils.atomicDataPathFn(id).toFile
    val idStr = id.id
    val fileLength =  {id match {
      case id: DoubleScalarId => 8
      case id: IntScalarId => 4
      case id: BooleanScalarId => 1
    }}
    val mbb = (new RandomAccessFile(file, "r")).getChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength)
    id match {
      case id: DoubleScalarId => DoubleScalarData(mbb.getDouble())
      case id: IntScalarId => IntScalarData(mbb.getInt())
      case id: BooleanScalarId => BooleanScalarData(if (mbb.get().toInt == 0) false else true)
    }
  }

  override def putScalar(id: ScalarId, data: ScalarData[_]): Unit = {
    val file : File = storeUtils.atomicDataPathFn(id).toFile
    val idStr = id.id
    val fileLength =  {id match {
      case id: DoubleScalarId => 8
      case id: IntScalarId => 4
      case id: BooleanScalarId => 1
    }}
    val mbb = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength)
    data match {
      case data: DoubleScalarData => mbb.putDouble(data.data)
      case data: IntScalarData => mbb.putInt(data.data)
      case data: BooleanScalarData => mbb.put((if(data.data) 1 else 0).toByte)
    }
  }

  override def deleteScalar(id: ScalarId): Unit = ???

  override def putMatrix2D(id: Matrix2DId, data: Matrix2DData[_]): Unit = {
    if(id.isInstanceOf[BooleanMatrix2DId]){throw new NotImplementedError("Still need to write boolean matricies")}
    val file : File = storeUtils.atomicDataPathFn(id).toFile
    val idStr = id.id
    val fileLength = 4 + 4 + {id match {
      case id : DoubleMatrix2DId =>  +  8 * data.metadata.rows * data.metadata.cols
      case id : IntMatrix2DId =>   4 * data.metadata.rows * data.metadata.cols
      case id : BooleanMatrix2DId =>   data.metadata.rows * data.metadata.cols
    }}
    val mbb = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength)
    mbb.putInt(data.metadata.rows)
    mbb.putInt(data.metadata.cols)
    data.data.data match {
      case array : Array[Double] => {
        val mdb: DoubleBuffer = mbb.asDoubleBuffer
        mdb.put(array)
      }
      case array : Array[Int] => {
        val mib: IntBuffer = mbb.asIntBuffer()
        mib.put(array)
      }
      case _ => throw new NotImplementedError()
    }
  }

  override def deleteMatrix2D(id: Matrix2DId): Unit = ???


  override def getMatrix2D(id: Matrix2DId): Matrix2DData[_] = {
    if(id.isInstanceOf[BooleanMatrix2DId]){throw new NotImplementedError("Still need to write boolean vectors")}
    val file : File = storeUtils.atomicDataPathFn(id).toFile
    val idStr = id.id
    val mbbtmp = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_ONLY, 0, 8)
    val rows = mbbtmp.getInt()
    val cols = mbbtmp.getInt()
    val dataLength = rows * cols
    val fileLength = 4 + 4 + dataLength * {id match{
      case id : DoubleMatrix2DId => 8
      case id : IntMatrix2DId => 4
      case _ => throw new NotImplementedError()
    }}
    val mbb = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength)
    mbb.getInt()
    mbb.getInt()
    id match {
      case id : DoubleMatrix2DId => {
        val arr : Array[Double] = Array.ofDim[Double](dataLength)
        mbb.asDoubleBuffer().get(arr)
        DoubleMatrix2DData(new DenseMatrix[Double](rows, cols, arr))
      }
      case id : IntMatrix2DId => {
        val arr : Array[Int] = Array.ofDim[Int](dataLength)
        mbb.asIntBuffer().get(arr)
        IntMatrix2DData(new DenseMatrix[Int](rows, cols, arr))
      }
      case _ => throw new NotImplementedError()
    }
  }

  override def putVector(id: VectorId, data: VectorData[_]): Unit = {
    if(id.isInstanceOf[BooleanVectorId]){throw new NotImplementedError("Still need to write boolean vectors")}
    val file : File = storeUtils.atomicDataPathFn(id).toFile
    val idStr = id.id
    val fileLength = id match {
      case id : DoubleVectorId => 4 +  8 * data.metadata.length
      case id : IntVectorId => 4 +  4 * data.metadata.length
      case id : BooleanVectorId => 4  +  data.metadata.length
    }
    val mbb = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength)
    mbb.putInt(data.metadata.length)
    data.data.data match {
      case array : Array[Double] => {
        val mdb: DoubleBuffer = mbb.asDoubleBuffer
        mdb.put(array)
      }
      case array : Array[Int] => {
        val mib: IntBuffer = mbb.asIntBuffer()
        mib.put(array)
      }
      case _ => throw new NotImplementedError()
    }
  }

  override def getVector(id: VectorId): VectorData[_] = {
    if(id.isInstanceOf[BooleanVectorId]){throw new NotImplementedError("Still need to write boolean vectors")}
    val file : File = storeUtils.atomicDataPathFn(id).toFile
    val idStr = id.id
    val mbbtmp = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_ONLY, 0, 4)
    val dataLength = mbbtmp.getInt()
    val fileLength = 4 + dataLength * {id match{
      case id : DoubleVectorId => 8
      case id : IntVectorId => 4
      case _ => throw new NotImplementedError()
    }}
    val mbb = (new RandomAccessFile(file, "rw")).getChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength)
    mbb.getInt()
    id match {
      case id : DoubleVectorId => {
        val arr : Array[Double] = Array.ofDim[Double](dataLength)
        mbb.asDoubleBuffer().get(arr)
        DoubleVectorData(DenseVector(arr))
      }
      case id : IntVectorId => {
        val arr : Array[Int] = Array.ofDim[Int](dataLength)
        mbb.asIntBuffer().get(arr)
        IntVectorData(DenseVector(arr))
      }
      case _ => throw new NotImplementedError()
    }

  }

  override def deleteVector(id: VectorId): Unit = ???



  def writeFullChannel(id : MultiChannelTimeSeriesId, channelId : TimeSeriesChannelId, data : Array[_], start : Int, length : Int, sfreq : BigDecimal) : Unit = {
    val channelFile : File = channelPathFn(id, channelId.toString).toFile
    val raf = new RandomAccessFile(channelFile, "rw")
    val outChannel = raf.getChannel
    val fileLength = id match {
      case id : DoubleMultiChannelTimeSeriesId => 8 + 4 + 8 * data.length
      case id : IntMultiChannelTimeSeriesId => 8 + 4 + 4 * data.length
      case id : BooleanMultiChannelTimeSeriesId => 8 + 4 +  data.length
    }
    val mbb : MappedByteBuffer= outChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength)
    mbb.putDouble(sfreq.toDouble)
    mbb.putInt(data.length)
    data match {
      case array : Array[Double] => {
        val mdb: DoubleBuffer = mbb.asDoubleBuffer
        mdb.put(array, start, length)
      }
      case array : Array[Int] => {
        val mib: IntBuffer = mbb.asIntBuffer()
        mib.put(array, start, length)
      }
      case array : Array[Boolean] => {
        val slice : Array[Byte] = array.slice(start, start+length).map(b  => (if(b) 1 else 0).toByte)
        mbb.put(slice)
      }
      case _ => throw new NotImplementedError()
    }
    raf.close()
    outChannel.close()
  }

  def readFullChannel(id : MultiChannelTimeSeriesId, channelId: TimeSeriesChannelId, array : Array[_], start : Int, length : Int) : Unit = {
    val channelFile : File = channelPathFn(id, channelId.toString).toFile
    val raf = new RandomAccessFile(channelFile, "r")
    val inChannel = raf.getChannel
    val mbb : MappedByteBuffer= inChannel.map(FileChannel.MapMode.READ_ONLY, 0, channelFile.length())
    val sfreq : BigDecimal = BigDecimal(mbb.getDouble)
    val nSamples : Int = mbb.getInt
    array match {
      case array : Array[Double] => {
        val mdb: DoubleBuffer = mbb.asDoubleBuffer
        mdb.get(array, start, length)
        mdb.clear()
      }
      case array : Array[Int] => {
        val mib: IntBuffer = mbb.asIntBuffer()
        mib.get(array, start, length)
        mib.clear()
      }
      case array : Array[Boolean] => {
        val tmpArray = Array.ofDim[Byte](length)
        mbb.get(tmpArray)
        tmpArray.map( b => if (b==0) false else true).copyToArray(array, start)
      }
    }
    mbb.clear()
    raf.close()
    inChannel.close()
  }

  override def putMultiChannelTimeSeries(id: MultiChannelTimeSeriesId, data: MultiChannelTimeSeriesData[_]): Unit = {
    val chansPath: Path = channelPathFn(id, channelsFileKey)
    Files.write(chansPath, data.channels.map(_.id).mkString("\n").getBytes(StandardCharsets.UTF_8))
    writeMultiChannelTimeSeriesMetadata(id, data.metadata)
    val futures = data match {
      case d: MultiChannelTimeSeriesData[_] => {
        d.channels.zipWithIndex.map { case (chanId: TimeSeriesChannelId, i: Int) => {
          implicit val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)
          Future {
            writeFullChannel(id, chanId, d.data.data, i * d.data.rows, d.data.rows, d.metadata.sfreq)
          }
        }
        }
      }
      case _ => throw new NotImplementedError()
    }
    Await.ready(Future.sequence(futures), Duration.Inf)
  }

  override def deleteMultiChannelTimeSeries(id: MultiChannelTimeSeriesId): Unit = ???

  override def getMultiChannelTimeSeries(id: MultiChannelTimeSeriesId): MultiChannelTimeSeriesData[_] = {
    val channels : Vector[TimeSeriesChannelId] = this.getChannels(id)
    val metadata : MultiChannelTimeSeriesMetadata[_] = this.getMultiChannelTimeSeriesMetadata(id)
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
        DoubleMultiChannelTimeSeriesData(data, Vector.tabulate[Timestamp](metadata.length){i : Int => Timestamp(BigDecimal(i)/metadata.sfreq)}, channels)
      }
      case id : IntMultiChannelTimeSeriesId =>{
        val dataArray = Array.ofDim[Int](metadata.length * channels.length)
        val futures = channels.zipWithIndex.map{case (chanId : TimeSeriesChannelId, i : Int) => {
          implicit val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)
          Future {
            readFullChannel(id, chanId, dataArray, i * metadata.length, metadata.length)
          }
        }
        }
        val data : DenseMatrix[Int] = new DenseMatrix[Int](metadata.length, channels.length, dataArray)
        Await.ready(Future.sequence(futures), Duration.Inf)
        IntMultiChannelTimeSeriesData(data, Vector.tabulate[Timestamp](metadata.length){i : Int => Timestamp(BigDecimal(i)/metadata.sfreq)}, channels)
      }
      case id : BooleanMultiChannelTimeSeriesId =>{
        val dataArray = Array.ofDim[Boolean](metadata.length * channels.length)
        val futures = channels.zipWithIndex.map{case (chanId : TimeSeriesChannelId, i : Int) => {
          implicit val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)
          Future {
            readFullChannel(id, chanId, dataArray, i * metadata.length, metadata.length)
          }
        }
        }
        val data : DenseMatrix[Boolean] = new DenseMatrix[Boolean](metadata.length, channels.length, dataArray)
        Await.ready(Future.sequence(futures), Duration.Inf)
        BooleanMultiChannelTimeSeriesData(data, Vector.tabulate[Timestamp](metadata.length){i : Int => Timestamp(BigDecimal(i)/metadata.sfreq)}, channels)
      }

      case _ => throw new NotImplementedError("Type of timeseries doesn't exist")
    }
  }

  def writeMultiChannelTimeSeriesMetadata(id : MultiChannelTimeSeriesId, meta : MultiChannelTimeSeriesMetadata[_]) : Unit = {
    Files.write(storeUtils.channelPathFn(id, timesFileKey),
      Array(meta.startTime.toString, meta.sfreq.toString, meta.length.toString).mkString("\n").getBytes(StandardCharsets.UTF_8))
  }

  def getMultiChannelTimeSeriesMetadata(id : MultiChannelTimeSeriesId) : MultiChannelTimeSeriesMetadata[_] = {
    val timeMetadata = Files.readAllLines(channelPathFn(id, timesFileKey)).asScala.toVector
    id match {
      case id : DoubleMultiChannelTimeSeriesId => new MultiChannelTimeSeriesMetadata[Int](timeMetadata(1).toInt, BigDecimal(timeMetadata(0)))
      case id : IntMultiChannelTimeSeriesId => new MultiChannelTimeSeriesMetadata[Int](timeMetadata(1).toInt, BigDecimal(timeMetadata(0)))
      case id : BooleanMultiChannelTimeSeriesId => new MultiChannelTimeSeriesMetadata[Boolean](timeMetadata(1).toInt, BigDecimal(timeMetadata(0)))
      case _ => throw new NotImplementedError()
    }
  }

  def getChannels(id: MultiChannelTimeSeriesId, group : String = "meg"): Vector[TimeSeriesChannelId] = {
    val chansPath : Path = channelPathFn(id, channelsFileKey)
    val chans = Files.readAllLines(chansPath).asScala.toVector.map(TimeSeriesChannelId(_))
    group match {
      case "all" => chans
      case "meg" => chans.slice(0,306)
    }
  }

  override def getMultiChannelTimeSeriesWindow(id: MultiChannelTimeSeriesWindowId): MultiChannelTimeSeriesWindowData[_] = {
    null
  }
}

