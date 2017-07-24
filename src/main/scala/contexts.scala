package main.scala.contexts

import main.scala.store._

object EggsAlphaContext {
  def apply() : AlphaContext= {
    val binStore = new BinStore("/Users/dhowarth/work/db/bindata/")
    val binRegistry = new BinRegistry("/Users/dhowarth/work/db/bindata/")
    new  AlphaContext(binRegistry, binStore)
  }
}

