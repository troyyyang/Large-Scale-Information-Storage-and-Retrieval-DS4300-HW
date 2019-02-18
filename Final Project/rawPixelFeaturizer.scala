package ds4300
// importing libraries
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions.{col, udf}
import scala.collection.mutable




// class for the raw pixels featurizer
class rawPixelFeaturizer(override val uid: String) extends Transformer{

  // definition, setter and getter for the inputCol param
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final def getInputCol: String = $(inputCol)
  final def setInputCol(value: String): rawPixelFeaturizer = set(inputCol, value)

  // definition, setter and getter for the outputCol param
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final def getOutputCol: String = $(outputCol)
  final def setOutputCol(value: String): rawPixelFeaturizer = set(outputCol, value)

  // load image from a given path
  private def loadImage(path: String): BufferedImage = {
    ImageIO.read(new File(path))
  }

  // convert an image to grayscale and resize it
  private def resizeGreyscaleImage(image: BufferedImage):
  BufferedImage = {
    val resized = new BufferedImage(150, 150, BufferedImage.TYPE_BYTE_GRAY)
    val g = resized.getGraphics()
    g.drawImage(image, 0, 0, 150, 150, null)
    g.dispose()
    resized
  }

  // convert a bufferedimage to an array
  private def imageToArray(image: BufferedImage): Array[Double] = {
    val width = image.getWidth
    val height = image.getHeight
    val pixels = Array.ofDim[Double](width * height)
    image.getData.getPixels(0, 0, width, height, pixels)
  }

  // wraps and executes the previous three methods
  private def extractPixels(path: String): Array[Double] = {
    val img = loadImage(path)
    val resized = resizeGreyscaleImage(img)
    imageToArray(resized)
  }

  // performing the main trainsformation, create a UDF and then apply it to a certain column
  override def transform(dataset: Dataset[_]): DataFrame ={
    val featurize = udf(extractPixels _)
    val vector = udf((features: mutable.WrappedArray[Double]) => Vectors.dense(features.toArray))
    dataset.select(col("*"),vector(featurize(col($(inputCol)))).as($(outputCol)))
  }

  // creates a copy of this instance, required by all transformers
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  // check input validity and adds to the existing schema
  override def transformSchema(schema: StructType): StructType = {
    val inputtype = schema($(inputCol)).dataType
    if (inputtype != DataTypes.StringType) {
      throw new Exception(s"Input type ${inputtype} did not match input type StringType")
    }
    schema.add(StructField($(outputCol), SQLDataTypes.VectorType, false))
  }
}
