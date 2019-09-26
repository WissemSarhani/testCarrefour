package Gen_data

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Random}


object Generator_data {

  private val ALPHA_NUM_STRING = "abcdefghijklmnopqrstuvxyzw0123456789"
 
  /** Générer un alphanumérique aléatoire
    * @return un alphanumérique aléatoire de longeur length
    */
  def randomAlphaNumeric(length: Int): String = {
    val builder = new StringBuilder
    var i = 0
    while (i < length) {
      i += 1
      val character = (Math.random * ALPHA_NUM_STRING.length).toInt
      builder.append(ALPHA_NUM_STRING.charAt(character))
    }
    builder.toString
  }


  /** Générer un id magasin alphanumérique aléatoire.
    */
  def generateRandomIdMagasin: String = {
    randomAlphaNumeric(8) + "-" +
      randomAlphaNumeric(5) + "-" +
      randomAlphaNumeric(4) + "-" +
      randomAlphaNumeric(4) + "-" +
      randomAlphaNumeric(12)
  }

  /** Générer une date aléatoire
    *
    * @return une date aléatoire
    */
  def Rand_Date_Gen(n: Int): String = {
    val Simple_Date = new SimpleDateFormat("yyyyMMdd'T'HHmmssZ")
    val result = new Date(System.currentTimeMillis + n * 3452)
    Simple_Date.format(result)
  }

  /** Générer un fichier de transactions
    *
    * @param path        la destination du fichier
    * @param date        la date des transactions
    * @param linesNumber le nombre des lignes dans le fichier
    *
    */

  def Trans_Files_Gen(path: String, date: String, linesNumber: Int) {

    val writer = new PrintWriter(new File(path))
    var i = 0
    while (i < linesNumber) {
      val rand = new Random
      val transId = Math.abs(rand.nextInt(100))
      val n = Math.abs(rand.nextInt(100))
      val Rand_Time = Rand_Date_Gen(n)
      val datetime = date + Rand_Time.substring(8, Rand_Time.toString.length)
      val Id_Magasin = generateRandomIdMagasin
      val Id_Product = Math.abs(rand.nextInt(100))
      val qte = Math.abs(rand.nextInt(100))
      writer.write(transId + "|" + datetime + "|" + Id_Magasin + "|" + Id_Product + "|" + qte + "\n")
      i += 1
    }
    writer.close()
  }

  /** Générer un fichier de references
    *
    * @param path        la destination du fichier
    * @param linesNumber le nombre des lignes dans le fichier
    *
    */

  def Ref_File_Gen(path: String, linesNumber: Int) {
    val writer = new PrintWriter(new File(path))
    var i = 0
    while (i < linesNumber) {
      val rand = new Random
      val Product = Math.abs(rand.nextInt(100))
      val Price = "%05.2f".format(rand.nextFloat * 100) + ""
      writer.write(Product + "|" + Price.substring(0, 5) + "\n")
      i += 1
    }
    writer.close()
  }
}
