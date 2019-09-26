package DataUses

import Results_Process._

object Process {

  def main(args: scala.Array[String]) {

    val date = args(0)
    val Num_Days = args(1).toInt
    val input_Path = args(2)
    val output_Path = args(3)

    process(date, input_Path, output_Path, Num_Days)

  }

}






