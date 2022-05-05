package br.com.geekfox.twitter

import br.com.geekfox.twitter.exceptions.ArgumentNotFoundException
import br.com.geekfox.twitter.properties.AppProperties
import br.com.geekfox.twitter.spark.SparkSessionSingleton
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, Tokenizer}
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    //primeira coisa a fazer, configurar arquivo de configuracao
    if (args.length == 0) {
      throw new ArgumentNotFoundException("O arquivo de propriedades nao foi informado")
    }
    val file = args(0)

    AppProperties.setConfig(file)

    val config: Properties = AppProperties.getConfig()

    val spark: SparkSession = SparkSessionSingleton.getInstance()

    import spark.implicits._

    //carrega tweets do conjunto de treinamento
    val tweets_df = spark.read.format("csv").load("s3://" + config.getProperty("model.trainingData"))

    val df = tweets_df
      .selectExpr("CAST(_c0 AS INT)", "_c5")
      .map(row => (if (row.getInt(0) == 4) 1 else 0, row.getString(1)))
      .selectExpr("_1 AS label", "_2 AS tweet")

    //cria pipeline
    val tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("words")
    val vectorizer = new CountVectorizer().setInputCol("words").setOutputCol("features")
    val estimator = new NaiveBayes().setFeaturesCol("features").setLabelCol("label")
    val pipeline = new Pipeline().setStages(Array(tokenizer, vectorizer, estimator))

    val nbModel = pipeline.fit(df)

    //salva modelo treinado
    val dateformat = new SimpleDateFormat("yyyyMMddHHmm")
    val key = "output_" + dateformat.format(Calendar.getInstance().getTime)
    nbModel.write.overwrite().save("s3://" + config.getProperty("model.pipelineOutput") + "/" + key)

    //carrega e modifica dados de teste
    val test_df = spark
      .read
      .format("csv")
      .load("s3://" + config.getProperty("model.testData"))
      .selectExpr("CAST(_c0 AS INT)", "_c5")
      .where("_c0 != 2")
      .map(row => (if (row.getInt(0) == 4) 1 else 0, row.getString(1)))
      .selectExpr("_1 AS label", "_2 AS tweet")

    val result_df = nbModel.transform(test_df)

    //avalia performance com acuracia
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(result_df)

    //salva no DynamoDB
    val jobConf: JobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
    jobConf.set("dynamodb.servicename", "dynamodb")
    jobConf.set("dynamodb.input.tableName", config.getProperty("model.statisticsTable"))
    jobConf.set("dynamodb.output.tableName", config.getProperty("model.statisticsTable"))
    jobConf.set("dynamodb.endpoint", "dynamodb.sa-east-1.amazonaws.com")
    jobConf.set("dynamodb.regionid", "sa-east-1")
    jobConf.set("dynamodb.throughput.read", "1")
    jobConf.set("dynamodb.throughput.read.percent", "1")
    jobConf.set("dynamodb.version", "2011-12-05")

    jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBInputFormat")


    spark.sparkContext.parallelize(Array(1)).map(i => {
      val attributes:util.Map[String, AttributeValue] = new util.HashMap[String, AttributeValue]()
      attributes.put("identifier", new AttributeValue().withS("twittermodel-2022"))
      attributes.put("timestamp", new AttributeValue().withN(Calendar.getInstance().getTime.getTime.toString))
      attributes.put("accuracy", new AttributeValue().withN(accuracy.toString))
      attributes.put("s3_key", new AttributeValue().withS(key))

      val item:DynamoDBItemWritable = new DynamoDBItemWritable
      item.setItem(attributes)

      (new Text(""), item)
    }).saveAsHadoopDataset(jobConf)
  }
}
