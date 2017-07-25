package main.scala.contexts

import main.scala.store._
import java.net.InetAddress

object HostnameDependentAlphaContext {
  def apply() : AlphaContext= {
    val location = InetAddress.getLocalHost.getHostName match {
      case "eggs.pc.cs.cmu.edu" => "/Users/dhowarth/work/db/bindata/"
      case "cortex.ml.cmu.edu" => "/share/volume1/medulla-store/bin-data"
    }
    val binStore = new BinStore(location)
    val binRegistry = new BinRegistry(location)
    new  AlphaContext(binRegistry, binStore)
  }
}

