package ProcessingData

import java.io.File
import DataUses.Results_Process._
import org.scalatest.FunSuite

import scala.io.Source

class Process_Test extends FunSuite {

  test(testName = "Test indicators process") {

    val date = "20190815"
    val input_Path = "./src/resources/input_files/"
    val output_Path = "./src/resources/output_files/"

    process(date, input_Path, output_Path, 1)
    process(date, input_Path, output_Path, 7)


    //liste des fichiers produites par process
    val dir = new File(output_Path)
    val files1 = dir.listFiles((d, name) => name.endsWith(date + ".data")).map(_.toString())
    val files7 = dir.listFiles((d, name) => name.endsWith(date + "-J7.data")).map(_.toString())

    val files = files1 ++ files7

    for (file <- files) {
      val list = Source.fromFile(file)
      val rows = list.getLines().map(line => line.split('|')).toList
      assert(rows.length == 100)
    }
  }
}
