package DataUses

import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import DataUses.Files_Uses._
import akka.actor.ActorSystem

object Results_Process {

  /** Répondre à tous les besoin:
    * - En passant days_number 1 on obtient les 4 premiers besoins 1,2,3,4
    * - En passant days_number 7 on obtient les 4 derniers besoins 4,5,6,7
    *
    * @param date         la date limite du calcul
    * @param input_Path       le dossier contenant les données en entrée
    * @param output_Path      le dossier du résultat
    * @param days_number le nombre de jours sur lequel on va effectuer le calcul
    */
  def process(date: String, input_Path: String, output_Path: String, days_number: Int): Unit = {

    implicit val system: ActorSystem = ActorSystem("Sys")
    val settings = ActorMaterializerSettings(system)
    implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

    var suffix: String = ""
    days_number match {
      case 1 => suffix
      case 7 => suffix = "-J7"
      case _ => println("enter a valid number of days : 1 or 7")
    }

    val result = concatFiles(input_Path, date, days_number)
    result.onComplete(x => {
      var transactions = x.get.map(line => line.split('|')).toList
      var top100VenteGlobale: List[(String, Int)] = Nil
      var top100CaGlobale: List[(String, Float)] = Nil

      val groupedByMagasin: Unit = transactions
        .filter(x => x(3) != "0")
        .groupBy(x => x(2))
        .foreach(x => {
          val Id_Magasin = x._1
          val groupedByMagVal = x._2
          val groupedByProd = groupedByMagVal.groupBy(x => x(3)).toList.map(x => {
            val productId = x._1
            val groupedByProdVal = x._2
            var qte: Int = 0
            var date = ""

            groupedByProdVal.foreach(x => {
              qte += x(4).toInt
              date = x(1).slice(0, 8)
            })

            // Récuperer le Price par date et par Product
            val priceReference = scala.io.Source.fromFile(input_Path + "reference_prod-" + Id_Magasin + "_" + date + ".data").
              getLines().map(line => line.split('|')).toList

            val price = priceReference.filter(x => x(0) == productId)(0)(1).toFloat
            val chiffreAffaire = price * qte

            (productId, qte, chiffreAffaire)

          })

          // Calculer les top 100 vente pour un magasin
          val top100VenteParMagasin = groupedByProd.map(x => (x._1, x._2)).sortBy(-_._2).take(100)

          // Calculer les top 100 CA pour un magasin
          val top100CaParMagasin = groupedByProd.map(x => (x._1, x._3)).sortBy(-_._2).take(100)

          export(top100VenteParMagasin, output_Path + "top_100_ventes_" + Id_Magasin + "_" + date + suffix + ".data")
          export(top100CaParMagasin, output_Path + "top_100_ca_" + Id_Magasin + "_" + date + suffix + ".data")

          // Concatiner seulement les 100 premiers de chaque magasin pour réduire les ligne traitées.
          top100VenteGlobale = top100VenteGlobale ++ top100VenteParMagasin
          top100CaGlobale = top100CaGlobale ++ top100CaParMagasin

        })

      val top100VenteGlobales = top100VenteGlobale.sortBy(-_._2).take(100)
      val top100CaGlobales = top100CaGlobale.sortBy(-_._2).take(100)

      // Exporter les top 100 ventes global et top 100 ca global
      export(top100VenteGlobales, output_Path + "top_100_vente_GLOBAL_" + date + suffix + ".data")
      export(top100CaGlobales, output_Path + "top_100_ca_GLOBAL_" + date + suffix + ".data")

      // Terminer Akka actor.
      system.terminate()
      System.exit(0)
    })
  }
}
