package ProcessingData

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.scalatest.FunSuite
import DataUses.Files_Uses._

import scala.concurrent.ExecutionContext.Implicits.global




class Files_Uses_Test extends FunSuite {


  test(testName = "Test read Stream") {

    val stream = readStream("./src/resources/input_files/", "20190815")
    var result: List[scala.Array[String]] = Nil
    stream.onComplete(x => {
      result = x.get.map(line => line.split('|')).toList
      assert(result.length == 45906)
    })
  }



  test(testName = "Test export files") {
    val path = new File("src/ressources/output_files").getAbsoluteFile.toString

    val list: List[(String, Any)] = List(("v1", "v2"), ("v3", "v4"), ("v5", "v6"))
    export(list, path + "/testExport")
    val exportedFile: List[scala.Array[String]] = scala.io.Source.fromFile(path + "/testExport")
      .getLines().map(line => line.split('|')).toList
    assert(exportedFile.length == 3)
  }

  test(testName = "Test concat Files") {

    implicit val system: ActorSystem = ActorSystem("Sys")
    val settings = ActorMaterializerSettings(system)
    implicit val materializer: ActorMaterializer = ActorMaterializer(settings)
    val result = concatFiles("./src/resources/input_files/", "20190815", 2)
    result.onComplete(x => {
      var transactions = x.get.map(line => line.split('|')).toList
      assert(transactions.length == 91812)

    })

  }

}
