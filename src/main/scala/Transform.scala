import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scalaj.http.Http
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._

object Transform {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ETL")
      .getOrCreate()

    import spark.implicits._  
    val matchs = spark.read.option("multiline","true").json("match.json")
    matchs.printSchema()
  //le nombre de points marqués par l'équipe, l'id du match, le nom et l'id de l'équipe. 
    matchs.createOrReplaceTempView("matchs")
    val df_match = spark.sql("SELECT data.home_team_score, data.id AS match_id, data.home_team.name, data.home_team.id FROM matchs")
    df_match.show()

    val stats = spark.read.option("multiline","true").json("stats.json")
    stats.printSchema()
    //les équipes spécifiées. Par la suite, de ces données, il sera nécessaire de choisir les statistiques des 
    //joueurs comme les points marquées, les passes réalisées, etc. Enfin, groupez les lignes par l'équipe et l'id du match.
    stats.createOrReplaceTempView("stats")
    val df_stats = spark.sql("SELECT data.game.id AS match_id2, data.player.first_name, data.pts, data.reb FROM stats")
    df_stats.show()

    df_match.printSchema()
    df_stats.printSchema()

    val jointure = df_match.join(df_stats,df_match("match_id")===df_stats("match_id2"),"full")
    jointure.show()
    jointure.write.csv("test.csv")
/*
    val group = jointure
    .select(explode($"match_id").as("match_id"), explode($"pts").as("pts"),explode($"name").as("name"),explode($"reb").as("reb"),explode($"first_name").as("first_name")) // Extraction des éléments du tableau
    .groupBy("match_id")
    .agg(sum("pts").as("total_pts")) // Calcul de la somme des points

    group.show()
*/
/*
    val schema = new StructType()
      .add("match_id2", LongType)
      .add("first_name", StringType)
      .add("pts", LongType)
      .add("reb", LongType)

    val parsedStats = stats.select(from_json($"value", schema).as("data")).select("data.*")
    val parsedStatsNumeric = parsedStats.withColumn("pts", $"pts".cast(IntegerType))
    val jointure = df_match.join(parsedStatsNumeric, df_match("match_id") === parsedStatsNumeric("match_id2"), "full")
    val group = jointure.groupBy("match_id").agg(sum("pts").as("total_pts"), sum("reb").as("total_reb"))
    group.show()
*/

    /*
    val jointure = df_match.join(df_stats,df_match("match_id")===df_stats("match_id2"),"full")
    jointure.show()
    val group = jointure.groupBy("match_id").sum("pts","rbd")
    group.show()
    */
  }
}