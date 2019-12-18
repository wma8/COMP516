import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val MASTER_ADDRESS = "ec2-3-88-144-202.compute-1.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val INPUT_DIR = HDFS_MASTER + "/wordcount/input"
        val OUTPUT_DIR = HDFS_MASTER + "/wordcount/output"

        val links_file = INPUT_DIR + "/links-simple-sorted.txt"
        val titles_file = INPUT_DIR + "/titles-sorted.txt"
        val output_file = OUTPUT_DIR + "/pagerank.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster(SPARK_MASTER)

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
            .map(s => (s.split(":")(0), s.split(":")(1)))
            .map{
                s => 
                val tmp = s._2.split(" ")
                    .filter(_ != "")
                (s._1, tmp)
            }

        // Titles with the index
        val titles = sc
            .textFile(titles_file, num_partitions)
            .zipWithIndex
            .map(s => ((s._2 + 1).toString(), s._1))
        
        // Find the URL without outlinks
        // NO outlink means it can links everywhere including itself
        // According to Markov Chain Random Process
        val noOutLinks = titles.subtractByKey(
            links.map(s => (s._1, "a"))
        )

        val numOfPages = titles.count()
        val emptyRank = titles.map(s => (s._1, 0.0))

        var ranks = titles
            .map(s => (s._1, 100.0 / numOfPages))

        for(i <- 1 to iters) {
            val noOutRanks = noOutLinks.join(ranks)
                .map(s => s._2._2/numOfPages)
                .reduce((x, y) => x + y)

            var contributions = links.join(ranks)
                .flatMap {case (url, (links, rank)) => 
                    links.map(dest => (dest, rank / links.size))
                }
            val temp2 = contributions.reduceByKey(_+_);
            val tmp = emptyRank.subtractByKey(temp2)
            ranks = tmp.union(temp2)
                .map(s => (s._1, 0.15/numOfPages * 100 + 0.85 * (s._2 + noOutRanks)))
        }
        
        //ranks.saveAsTextFile(OUTPUT_DIR)
        val result = titles.join(ranks)
            .sortBy(-_._2._2)
	    .take(10)
	    
	
	println("\n\n\n\n\n\n")
	result.foreach(println)

        // println("\n\n\n\n\n")
        // result.foreach(println)
        // // result.collect().foreach(println)
        

    }
}
