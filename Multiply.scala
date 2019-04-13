import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
	val conf = new SparkConf().setAppName("Matrix Multiplication on Spark")
	val sc = new SparkContext(conf)

	val matrix_m = sc.textFile(args(0)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )	

	val matrix_n = sc.textFile(args(1)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )

	val MultiplyMxN = matrix_m.map( matrix_m => (matrix_m._2, matrix_m)).join(matrix_n.map( matrix_n => (matrix_n._1,matrix_n)))
									 .map{ case (k, (matrix_m,matrix_n)) => 
										((matrix_m._1,matrix_n._2),(matrix_m._3 * matrix_n._3)) }

	val ReduceSort = MultiplyMxN.reduceByKey((m,n) => (m+n)).sortByKey(true, 0)

	val WriteData = ReduceSort.map(rdd => rdd._1._1 + " " + rdd._1._2 + " " + rdd._2).saveAsTextFile(args(2))

	sc.stop()
	
  }
}