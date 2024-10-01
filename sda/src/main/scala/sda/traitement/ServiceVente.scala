package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

 /*  def calculTTC () : DataFrame
✓ Calcule le TTC => HTT+TVA*HTT, le TTC doit être arrondi à 2 chiffres après la virgule
✓ Supprime la colonne TVA, HTT//
 */
    def calculTTC () : DataFrame ={
      dataFrame
    .withColumn("HTT", regexp_replace(col("HTT"), ",", ".").cast("double"))
    .withColumn("TVA", regexp_replace(col("TVA"), ",", ".").cast("double"))
    .withColumn("TTC", round(col("HTT") * col("TVA") + col("HTT"), 2))
    .drop("HTT","TVA")
    }

/*
✓ Créer une nouvelle colonne Date_End_contrat et Ville en utilisant la méthode
select from_json et regexp_extract pour extraire YYYY-MM-DD
✓ Supprime la colonnemetaData
 */
    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)
      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)
      /*..........................coder ici...............................*/
      val dfData = dataFrame.withColumn("data", from_json(col("MetaData"), schema))

      val dfExploded = dfData.select(col("*"),explode(col("data.MetaTransaction")).alias("transaction"))

      val dfDateVille = dfExploded.select(
         col("*"),
         regexp_extract(col("transaction.Date_End_contrat"), "(\\d{4}-\\d{2}-\\d{2})", 1).alias("Date_End_contrat"),
         regexp_extract(col("transaction.Ville"), "(\\w+)", 1).alias("Ville")
          )
        .drop("MetaData","data","transaction")
        .na.drop("any")

        dfDateVille
        }
/*
Créer un nouvelle colonne Contrat_Status avec "Expired" si le contrat a expiré et
sinon "Actif"
 */
    def contratStatus(): DataFrame = {
      /*..........................coder ici...............................*/
      dataFrame.withColumn("Contrat_Status", when(col("Date_End_contrat").cast("date") < current_date(), "Expired").otherwise("Active")
      )
    }
  }

}