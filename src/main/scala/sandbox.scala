import main.scala.core._
import main.scala.store._

import java.lang.Runtime
import java.lang.System


object GetUELsAndStimuli {
  def apply(metadataDir : String, subject : String, experiment : String ) ={
    val blocks : Vector[String] = experiment match {
      case "prep-role-1" => Vector("01", "02", "03", "04", "05", "06", "07", "08")
      case _ => throw new Error(s"Unimplemented experiment $experiment")
    }
  }
}



/**
  * Created by dhowarth on 6/30/17.
  */
object Sandbox {
  def main(args: Array[String]): Unit = {
    val bs = new BinStore("/Users/dhowarth/work/db/bindata/", 400)
    println("beforebefore")
    println(Runtime.getRuntime.availableProcessors)
    val t = System.currentTimeMillis()
    val d = bs.getMultiChannelTimeSeries(DoubleMultiChannelTimeSeriesId("test:A:01:raw"))
    println( "Time ", (System.currentTimeMillis()-t)/1000.0)
    println(d.data(0 to 10,0 to 10))
    println(d.data.rows, d.data.cols)
    println(Runtime.getRuntime.totalMemory)
  }
}