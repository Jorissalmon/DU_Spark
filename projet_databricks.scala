
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import spark.sqlContext.implicits
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.expressions._

def readerjson(path: String)(implicit spark: SparkSession): Seq[DataFrame]={
  val format = "json"
   val df = spark.read
    .format(format)
    .option("multiline", true)
    .load(path)
  
  val Transaction = df.select(explode(col("Transaction")).alias("Transaction")).select("Transaction.*")
  
  val Devis = df.select(explode(col("Devis")).alias("Devis")).select("Devis.*")

  Seq(Transaction, Devis)
}

implicit val spark: SparkSession = SparkSession.builder
  .master("local[*]")
  .getOrCreate()

val dataframes: Seq[DataFrame] = readerjson("/FileStore/tables/TP_Databricks.json")

val dftransaction: DataFrame=dataframes(0)
val dfdevis: DataFrame=dataframes(1)

def NbCommandeParProduitDansPays(df:DataFrame): DataFrame={
val result:DataFrame=df
.groupBy("TypeProduit")
.pivot("Pays")
.agg(count("*"))
.na.fill(0)
.withColumnRenamed("count","Nbcommandes")

result
}

NbCommandeParProduitDansPays(dftransaction).show

def PrixEur(dftransaction:DataFrame, dftauxdevis:DataFrame):DataFrame={
  val df=dftransaction.join(dftauxdevis, dftransaction("Devis")===dftauxdevis("Devis"))
  val df_euro=df.withColumn("PrixEur",$"Prix"*$"Taux")
  df_euro
}

val df_conv_eur=PrixEur(dftransaction,dftauxdevis)
df_conv_eur.show

def Top2TransactionPerPays(df:DataFrame):DataFrame={
  val windowSpec= Window.partitionBy("Pays").orderBy(col("Prix").desc)

  val df_rank = df.withColumn("rank", row_number().over(windowSpec))

  val Top2TransactionPerPays = df_rank.filter(col("rank") <= 2)
 Top2TransactionPerPays
}

val Top_par_pays=Top2TransactionPerPays(df_conv_eur)
Top_par_pays.show

def Cube (df:DataFrame):DataFrame={
  val cube: DataFrame= df.cube("Pays","TypeProduit")
  .sum("PrixEur")
  .withColumnRenamed("sum(PrixEur)","Chiffre_Affaire_EUR")
  .na.fill("AllPays", Seq("Pays"))
  .na.fill("AllProduit", Seq("TypeProduit"))
  cube
}

val cubeResult:DataFrame=Cube(df_conv_eur)
cubeResultCube.show

df_conv_eur.createOrReplaceTempView("MSD")

%sql
select 
  concat(  coalesce(TypeProduit, "ALL_Product"),
  "-",
  coalesce(Pays, "ALL_Pays")),
  SUM(PrixEur) as Chiffre_Affaire_EUR
 from MSD GROUP BY CUBE (Pays, TypeProduit)




