package br.com.geekfox.twitter.spark

import br.com.geekfox.twitter.properties.AppProperties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

object SparkSessionSingleton {
  @transient private var spark: SparkSession = _
  private val config:Properties= AppProperties.getConfig()

  /* Método responsável por inicializar ou retornar uma sessão spark
   * @return spark sessão spark
   */
  def getInstance(): SparkSession = {

    if (spark == null) {
      val sparkConfig: SparkConf = new SparkConf()
        .setAppName(config.getProperty("app.name"))
        .setMaster("yarn")


      spark = SparkSession
        .builder
        .config(sparkConfig)
        .getOrCreate
    }

    spark
  }

}
