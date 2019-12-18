import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.
import scala.util.control.Breaks._


object NoInOutLink {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        
        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

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

        println("\n\n\n\n")
        /* No Outlinks */
        // noOut.foreach(println)

        val no_out = titles.subtractByKey(joinList)
            .sortBy(_._1)
            .collect()

        val no_outlinks = no_out.size

        println("\n\n\n\n")
        println("[ NO OUTLINKS ]")

        var length1 = 0

        if (no_out.size > 10)
            length1 = 10
        else
            length1 = no_out.size - 1

        for(i <- 0 to length1) {
            // println(length1)
            println(no_out(i))
        }
        // noOut.collect().foreach(println)

        // for (n <- names) println(n)
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

        val no_in = titles.subtractByKey(joinList2)
            .sortBy(_._1).collect()

        val no_inlinks = no_in.size
        
        println("\n[ NO INLINKS ]")

        var length2 = 0

        if (no_in.size > 10)
            length2 = 10
        else
            length2 = no_in.size - 1

        for(i <- 0 to length2) {
            println(no_in(i))
        }
        // noIn2.collect().foreach(println)

        println("\n\n\n\n")

    }
}
