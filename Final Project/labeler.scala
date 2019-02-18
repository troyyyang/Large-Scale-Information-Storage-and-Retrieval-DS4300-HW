package ds4300
// importing libraries
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions.{col, udf}

// class for the raw labeler
class labeler (override val uid: String) extends Transformer{

  // definition, setter and getter for the inputCol param
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")
  final def getInputCol: String = $(inputCol)
  final def setInputCol(value: String): labeler = set(inputCol, value)

  // definition, setter and getter for the outputCol param
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final def getOutputCol: String = $(outputCol)
  final def setOutputCol(value: String): labeler = set(outputCol, value)

  // extract the label from a path
  private def extractLabels(path: String): Int = {
    if(path.substring(46,49) == "dog") {
      1
    }
    else 0
  }

  // creates a copy of this instance, required by all transformers
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  // performing the main trainsformation, create a UDF and then apply it to a certain column
  override def transform(dataset: Dataset[_]): DataFrame ={
    val labels = udf(extractLabels _)
    dataset.select(col("*"),labels(col($(inputCol))).as($(outputCol)))

  }

  // check input validity and adds to the existing schema
  override def transformSchema(schema: StructType): StructType = {

    val inputtype = schema($(inputCol)).dataType
    if (inputtype != DataTypes.StringType) {
      throw new Exception(s"Input type ${inputtype} did not match input type StringType")
    }
    schema.add(StructField($(outputCol), DataTypes.IntegerType, false))
  }
}
