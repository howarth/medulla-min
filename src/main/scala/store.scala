package main.scala.store

import main.scala.core._

import main.java.constants.DataTypeStrings;
import scala.collection.JavaConverters._


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

