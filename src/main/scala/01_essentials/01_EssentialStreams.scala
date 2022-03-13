package essentials

import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleNumberStream.print()

    env.execute() // trigger all the computation that where described earlier
  }

  // transformations
  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int]        = env.fromElements(1, 2, 3, 4, 5)

    println(s"Current parallelism: ${env.getParallelism}")
    env.setParallelism(2)
    println(s"New parallelism: ${env.getParallelism}")

    // map
    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    // flatMap
    val expendedNumbers: DataStream[Int] = doubledNumbers.flatMap(n => List(n, n + 1))

    // filter
    val filteredNumbers: DataStream[Int] =
      expendedNumbers
        .filter(_ % 2 == 0)
        .setParallelism(4) // set new parallelism

    val finalData = expendedNumbers.writeAsText("output/expendedStream") // directory with "nb v-core" files
    finalData.setParallelism(3) // set parallelism in the sink
  }

  def main(args: Array[String]): Unit =
    // applicationTemplate()
    demoTransformations()
}
