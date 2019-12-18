import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
            .map(s => (s.split(":")(0), s.split(":")(1)))
            .map(
                s => ({
                    val t1 = s._2.split(" ")
                    val t2 = for (i <- 1 until t1.length) 
                        yield (s._1, t1(i))
                    (t2) }))
            .flatMap(links => links)
            // .flatMap(links => links)
            // .groupByKey().cache()  

        // Titles with the index
        val titles = sc
            .textFile(titles_file, num_partitions)
            .zipWithIndex
            .map(s => (s._2, s._1))
            .map(s => ((s._1 + 1).toString(), s._2))
        
        // Find the URL without outlinks
        // NO outlink means it can links everywhere including itself
        // According to Markov Chain Random Process
        val noOutLinks = titles.subtractByKey(
            links.map(s => (s._1, s._1)).join(titles)
        ).map(s => (s._1))
        // Links no out links to every pages including itself
        val pageIndex = noOutLinks.cartesian(titles.map(s =>(s._1)))

        // Combine the noINs with the hasLinks pages together
        val finalLinks = pageIndex.union(links)


        val numOfPages = titles.count()
        println("\n\n\n\n")

        val linksGroup = finalLinks
            .distinct()
            .groupByKey()
        var ranks = linksGroup
            .map(s => (s._1, 100.0 / numOfPages))

        for (i <- 1 to iters) {
            // Mapper with contributions and the outlinks
            val contributions = linksGroup
                .join(ranks)
                .map(s => (s._2._1, s._2._2))
                .flatMap{ case(outs, rank) => 
                    val outnum = outs.size
                    outs.map(s => (s, rank / outnum))    
                }
            // Reducer will put the weights to the inlinks using the Equation
            ranks = contributions.reduceByKey(_ + _)
                .map(s => (s._1, 0.15/numOfPages * 100 + 0.85 * s._2)) 
        
        }
        
        // ranks.collect().foreach(println)
        println("\n\n\n\n")
        // println("\n\n\n\n" + numOfPages)
        println("[ PageRanks ]")

        val results = ranks
            .join(titles)
            .map(s => (s._1, s._2._2, s._2._1))
            .sortBy(-_._3)
            .collect()

            var length = 0

            if (results.size > 10)
                length = 10
            else
                length = results.size - 1
        
        for (i <- 0 to length) {
            println(results(i))
        }
    }
}
