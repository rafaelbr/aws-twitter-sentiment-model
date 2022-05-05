package br.com.geekfox.twitter.properties

import java.io.FileInputStream
import java.util.Properties

object AppProperties extends Serializable {

  private var config:Properties = _

  def setConfig(file:String): Unit = {
    if (config == null) {
      config = new Properties
      config.load(new FileInputStream(file))
    }
  }

  def getConfig(): Properties = {
    if (config == null) {
      config = new Properties()
    }
    config
  }

}
