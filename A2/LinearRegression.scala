package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object LinearRegression {
	
  def main(args: Array[String]): Unit = {
    
    // We need to use spark-submit command to run this program
    val conf = new SparkConf().setAppName("A2_Q1").setMaster("local[2]").set("spark executor.memory","1g");
    val sc = new SparkContext(conf);
    
    // Load and parse training data 
	val data = sc.textFile("/home/bigdata/Downloads/A2.data")
	val parsedData = data.map { line => 
		val parts = line.split(',') 
		LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
	}.cache()
		
	val numIterations = 100
	val stepSize = 1
	val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
	
	//test the model
	val valuesAndPreds = parsedData.map { point =>
	val prediction = model.predict(point.features)
	(point.label, prediction)
	}
	
	valuesAndPreds.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))
	
	//calculate error
	val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
	println("training Mean Squared Error = " + MSE)

  }
}