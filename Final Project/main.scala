// importing libraries
import org.apache.spark.ml.classification.{LinearSVC, MultilayerPerceptronClassifier, NaiveBayes}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import ds4300._
import org.apache.spark.ml.Pipeline

// this is where I create instances of transformers and estimators, then create a pipeline with some configuration
// of them
object main extends App {

  // disable logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // relative path of the folder where the images are located
  val path = "1k"

  // create spark session
  val sc = SparkSession.builder().master("local").appName("sparkproject").getOrCreate()

  // getting all the files and their contents (the contents are meaningless)
  val rdd = sc.sparkContext.wholeTextFiles(path)

  // extracting just the file names
  val imagepaths = rdd.map { case (fileName, content) =>
    fileName.replace("file:", "")
  }

  import sc.implicits._

  // making a one column dataframe with all the image paths
  val imagestmp = imagepaths.toDF()
  imagestmp.show

  // splitting data into training and test sets
  val Array(training, test) = imagestmp.randomSplit(Array(0.8, 0.2), seed = 1)

  // creating the different transformers
  val cannyfeaturizer = new cannyEdgeFeaturizer("cannyfeaturizer").setInputCol("value").setOutputCol("pixels")
  val sobelfeaturizer = new sobelEdgeFeaturizer("sobelfeaturizer").setInputCol("value").setOutputCol("pixels")
  val laplacefeaturizer = new laplaceEdgeFeaturizer("laplacefeaturizer").setInputCol("value").setOutputCol("pixels")
  val rawfeaturizer = new rawPixelFeaturizer("rawfeaturizer").setInputCol("value").setOutputCol("pixels")
  val labeler = new labeler("labeler").setInputCol("value").setOutputCol("label")

  // creating the different estimators
  val nb = new NaiveBayes().setFeaturesCol("pixels").setLabelCol("label")
  val lsvm = new LinearSVC().setMaxIter(10).setRegParam(0.3)
    .setFeaturesCol("pixels").setLabelCol("label")
  // define the layers for the multilayer perceptron classifier, first number is the number of inputs, last is outputs
  // 22500 because the images are shrunk to 150 x 150, 150 * 150 = 22500, output is 2 because there are 2 classes
  val layers = Array[Int](22500, 5, 4, 2)
  val mpc = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setMaxIter(100)
    .setFeaturesCol("pixels").setLabelCol("label")

  // creating the pipeline, the array inside setStages is where the pipeline parts can be configured
  val pipeline = new Pipeline().setStages(Array(labeler,rawfeaturizer,nb))

  // fitting the pipeline on the training data and running it on the test
  val model = pipeline.fit(training)
  val result = model.transform(test)
  result.show

  // calculate and print accuracy
  val correct = result.filter($"prediction" === $"label").count()
  val total = test.count()
  println(correct.toFloat/total)

}