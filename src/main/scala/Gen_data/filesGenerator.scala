package Gen_data

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import Gen_data.Generator_data._

object filesGenerator {

  def main(args: scala.Array[String]): Unit = {

    val path = args(0)
    val date = args(1)
    val days_number = args(2).toInt
    val Trans_line_Num = args(3).toInt
    val Ref_line_Num = args(4).toInt
    val Date_Format = DateTimeFormatter.ofPattern("yyyyMMdd")
    val runningDay = LocalDate.parse(date, Date_Format)

    var i = 0

    while (i < days_number) {
      val date = runningDay
        .minusDays(i)
        .toString
        .replace("-", "")

      Trans_Files_Gen(path + "/transactions_" + date + ".data", date, Trans_line_Num)
      Ref_File_Gen(path + "/reference_prod-" + generateRandomIdMagasin + "_" + date + ".data", Ref_line_Num)
      i += 1
    }
  }
}
