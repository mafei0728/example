import com.mafei.udafs.myem
import com.mafei.udafs.myem.MyAverage
import com.mafei.utils.SparkSessionFactory
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions

case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Long, myem.Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: myem.Average = myem.Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: myem.Average, data: Long): myem.Average = {
    buffer.sum += data
    buffer.count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: myem.Average, b2: myem.Average): myem.Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: myem.Average): Double = reduction.sum.toDouble / reduction.count
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[myem.Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// Register the function to access it
object ClassTest03 extends App{
  val spark = SparkSessionFactory.getSpark

  val df = spark.read.json("examples/src/main/resources/employees.json")
  df.createOrReplaceTempView("employees")
  df.show()
  // +-------+------+
  // |   name|salary|
  // +-------+------+
  // |Michael|  3000|
  // |   Andy|  4500|
  // | Justin|  3500|
  // |  Berta|  4000|
  // +-------+------+

  val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
  result.show()
  // +--------------+
  // |average_salary|
  // +--------------+
  // |        3750.0|
  // +--------------+

}
