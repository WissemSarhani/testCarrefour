package GeneratingData

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import Gen_data.Generator_data._
import DataUses.Files_Uses._

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.FunSuite

import scala.io.Source
import java.io.File
import java.net.URL
import java.nio.file.Paths

class  Data_Gen_Test extends FunSuite {
  test(testName = "generation des fichiers") {


    val path = new File("src/ressources/input_files").getAbsoluteFile.toString


    val date = "20190815"
    val days_number = 7
    val Trans_line_Num = 1000
    val Ref_line_Num = 1000
    val Date_Format = DateTimeFormatter.ofPattern("yyyyMMdd")
    val runningDay = LocalDate.parse(date, Date_Format)

    var i = 0

    while (i < days_number) {
      val date = runningDay.minusDays(i).toString.replace("-", "")
      Trans_Files_Gen(path + "/transactions_" + date + ".data", date, Trans_line_Num)
      Ref_File_Gen(path + "/reference_prod-" + generateRandomIdMagasin + "_" + date + ".data", Ref_line_Num)
      i += 1
    }

    while (i < days_number) {

      val date = runningDay.minusDays(i).toString.replace("-", "")
      val transactionsStream = readStream(path + "/transactions_" + date + ".data", date)
      val refStream = readStream(path + "/reference_prod-" + generateRandomIdMagasin + "_" + date + ".data", date)

      var transactions: List[scala.Array[String]] = Nil
      var refs: List[scala.Array[String]] = Nil

      transactionsStream.onComplete(x => {
        transactions = x.get.map(line => line.split('|')).toList
        assert(transactions.length == Trans_line_Num)
      })


      refStream.onComplete(x => {
        refs = x.get.map(line => line.split('|')).toList
        assert(refs.length == Ref_line_Num)
      })
      i += 1
    }
  }
}