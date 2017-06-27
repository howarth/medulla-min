
object NBExample {

  def getEvents(experiment : String, subject : String): Unit ={

  }

  def main(args : Array[String]): Unit ={
    val events : TaggedEventSet = ()
    val firstWordEvents : TaggedEventSet= events.filter("USI", (s : String) => s contains "word-0")
    val groupedByStimulus = firstWordEvents.groupBy("stimulus")

  }
}