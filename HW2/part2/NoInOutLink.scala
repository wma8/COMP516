
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.
import scala.util.control.Breaks._


object NoInOutLink {
    def main(args: Array[String]) {
	val MASTER_ADDRESS = "ec2-3-88-144-202.compute-1.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/wordcount/input"
        val OUTPUT_DIR = HDFS_MASTER + "/wordcount/output"

        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        val num_partitions = 10
        
        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster(SPARK_MASTER)

        val sc = new SparkContext(conf)
        val links = sc
            .textFile(links_file, num_partitions)    
        val lines = links
            .map(s => (s.split(":")(0), s.split(":")(1)))

        val index = lines.map(s => (s._1, s._1))

        val titles = sc
            .textFile(titles_file, num_partitions)
            .zipWithIndex
            .map(s => (s._2, s._1))
            .map(s => ((s._1 + 1).toString(), s._2))
        
        val joinList = index.join(titles)
            .map(s => (s._1, s._2._2))

        val no_out = titles.subtractByKey(joinList)
	    .map(s => (s._1.toInt, s._2))
            .sortBy(_._1)
            .take(10)

        val no_outlinks = no_out.size

        val noIn = lines.map(
            s => ({
            val t1 = s._2.split(" ")
            val t2 = for (i <- 1 until t1.length) 
                yield (s._1, t1(i))
            (t2) })
        ).flatMap(noIn => noIn)
        .map(s => (s._2, s._1))
        

        val joinList2 = noIn.join(titles)
        .map(s => (s._1, s._2._2))
        .distinct()

        var no_in = titles.subtractByKey(joinList2)
	    .map(s => (s._1.toInt, s._2))
            .sortBy(_._1)
	    .take(10)
        
        println("[ NO INLINKS ]")
	println("\n\n\n\n\n\n\n")
	no_out.foreach(println)
	println("\n")
	no_in.foreach(println)
	
        

    }
}
