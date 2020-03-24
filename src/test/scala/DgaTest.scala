import com.yeph.bigdata.dga.centrality.{GraphFactory, KBetweenness}
import junit.framework.TestCase
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}

class DgaTest extends TestCase {
  var spark: SparkContext = _

  @Before
  override def setUp(): Unit = {
    spark = SparkSession
      .builder()
      .appName("AlgorithmTest")
      .master("local")
      .getOrCreate()
      .sparkContext
  }

  @Test
  def testBC(): Unit = {
    val path = "D:/test.txt"
    val graphFactory = new GraphFactory
    val coeff = Array(0.1, 0.2, 0.3)
    val graph = graphFactory.build(spark, path, coeff)
    val result = KBetweenness.run(graph)
    println(result.collect().take(100).mkString("\n"))
  }

  @After
  override def tearDown(): Unit = {
    spark.stop()
  }

}
