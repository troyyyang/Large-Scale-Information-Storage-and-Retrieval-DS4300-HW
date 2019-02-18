// import Try for attempted unit conversion
import scala.util.Try

object stats extends App {

  // function that takes in the file path of the csv, calculates and prints summary statistics
  def calculate_stats(file:String): Unit = {

    // putting the csv into an array of arrays
    val csv = io.Source.fromFile(file).getLines.map(_.split(",")).toArray

    print("Statistics for ")
    println(file)

    // getting the minimums for each column
    println("Minimums:")
    for (i <- 0 to csv(0).length - 1) {
      // only calculate if it is numeric
      if (Try(csv.map(_(i)).drop(1)(0).toDouble).isSuccess) {
        print(csv(0)(i))
        print(": ")
        print(csv.map(_(i)).drop(1).map(_.toDouble).reduceLeft(_ min _))
        print(" ")
      }
      else {}
    }
    println("")

    // getting the maximums for each column
    println("Maximums:")
    for (i <- 0 to csv(0).length - 1) {
      // only calculate if it is numeric
      if (Try(csv.map(_(i)).drop(1)(0).toDouble).isSuccess) {
        print(csv(0)(i))
        print(": ")
        print(csv.map(_(i)).drop(1).map(_.toDouble).reduceLeft(_ max _))
        print(" ")
      }
      else {}
    }
    println("")

    // getting the averages for each columns
    println("Averages:")
    for (i <- 0 to csv(0).length - 1) {
      // only calculate if it is numeric
      if (Try(csv.map(_(i)).drop(1)(0).toDouble).isSuccess) {
        print(csv(0)(i))
        print(": ")
        // sum / length
        print(csv.map(_(i)).drop(1).map(_.toDouble).sum/csv.map(_(i)).drop(1).map(_.toDouble).length)
        print(" ")
      }
      else {}
    }
    println("")

    // getting the number of non-null values per column
    println("Number of Non-Null Values:")
    for (i <- 0 to csv(0).length - 1) {
      print(csv(0)(i))
      print(": ")
      print(csv.map(_(i)).drop(1).filter(_ != "").length)
      print(" ")
    }
    println("")

    // getting the number of distinct values per column
    println("Number of Distinct Values:")
    for (i <- 0 to csv(0).length - 1) {
      print(csv(0)(i))
      print(": ")
      print(csv.map(_(i)).drop(1).distinct.length)
      print(" ")
    }
    println("")
  }

  // calling the function on the bank.csv file
  calculate_stats("bank.csv")
  println("")
  // calling the function on another csv to make sure it works for other files
  calculate_stats("outlookContinuous.csv")
}