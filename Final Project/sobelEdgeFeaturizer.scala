package ds4300
// importing libraries
import java.awt.image.BufferedImage
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions.{col, udf}
import org.bytedeco.javacpp.indexer.FloatIndexer
import org.bytedeco.javacpp.opencv_core._
import org.bytedeco.javacpp.{DoublePointer, opencv_imgcodecs}
import org.bytedeco.javacpp.opencv_imgcodecs.IMREAD_COLOR
import org.bytedeco.javacpp.opencv_imgproc.Sobel
import org.bytedeco.javacv.Java2DFrameConverter
import org.bytedeco.javacv.OpenCVFrameConverter.ToMat
import scala.collection.mutable



// class for the sobel edge detection featurizer
class sobelEdgeFeaturizer(override val uid: String) extends Transformer{

  // definition, setter and getter for the inputCol param
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final def getInputCol: String = $(inputCol)
  final def setInputCol(value: String): sobelEdgeFeaturizer = set(inputCol, value)

  // definition, setter and getter for the outputCol param
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final def getOutputCol: String = $(outputCol)
  final def setOutputCol(value: String): sobelEdgeFeaturizer = set(outputCol, value)

  // utility method I found online to convert a mat to one where pixels are represented as 8 bit unsigned integers
  private def toMat8U(src: Mat, doScaling: Boolean = true): Mat = {
    val minVal = new DoublePointer(Double.MaxValue)
    val maxVal = new DoublePointer(Double.MinValue)
    minMaxLoc(src, minVal, maxVal, null, null, new Mat())
    val min = minVal.get(0)
    val max = maxVal.get(0)
    val (scale, offset) = if (doScaling) {
      val s = 255d / (max - min)
      (s, -min * s)
    } else (1d, 0d)

    val dest = new Mat()
    src.convertTo(dest, CV_8U, scale, offset)
    dest
  }

  // utility method I found online to convert a vector of Point2f to a Mat representing a vector of Points2f
  private def toMat(points: Point2fVector): Mat = {
    val size: Int = points.size.toInt
    val dest = new Mat(1, size, CV_32FC2)
    val indx = dest.createIndexer().asInstanceOf[FloatIndexer]
    for (i <- 0 until size) {
      val p = points.get(i)
      indx.put(0, i, 0, p.x)
      indx.put(0, i, 1, p.y)
    }
    dest
  }

  // utility method I found online to convert a javaCV array to a bufferedimage
  private def toBufferedImage(mat: Mat): BufferedImage = {
    val openCVConverter = new ToMat()
    val java2DConverter = new Java2DFrameConverter()
    java2DConverter.convert(openCVConverter.convert(mat))
  }

  // read in the image, perform sobel edge detection, and then return a buffered image of the edges
  private def detectEdges(path: String): BufferedImage = {
    val newpath = path.substring(43)
    val pic = opencv_imgcodecs.imread(newpath, IMREAD_COLOR)
    val sobelX = new Mat()
    Sobel(pic, sobelX, CV_32F, 1, 0)
    val sobelY = new Mat()
    Sobel(pic, sobelY, CV_32F, 0, 1)
    val sobel = sobelX.clone()
    magnitude(sobelX, sobelY, sobel)
    val min = new DoublePointer(1)
    val max = new DoublePointer(1)
    minMaxLoc(sobel, min, max, null, null, new Mat())
    toBufferedImage(toMat8U(sobel))
  }

  // resize the image
  private def resizeImage(image: BufferedImage):
  BufferedImage = {
    val resized = new BufferedImage(150, 150, BufferedImage.TYPE_BYTE_GRAY)
    val g = resized.getGraphics()
    g.drawImage(image, 0, 0, 150, 150, null)
    g.dispose()
    resized
  }

  // convert a buffered image to an array of doubles
  private def getPixelsFromImage(image: BufferedImage): Array[Double] = {
    val width = image.getWidth
    val height = image.getHeight
    val pixels = Array.ofDim[Double](width * height)
    image.getData.getPixels(0, 0, width, height, pixels)
  }

  // wraps and executes the previous three methods
  private def extractPixels(path: String): Array[Double] = {
    val edges = detectEdges(path)
    val resized = resizeImage(edges)
    getPixelsFromImage(resized)
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
