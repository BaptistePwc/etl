import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import scalaj.http.Http
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.lit

object Extract {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ETL")
      .getOrCreate()
/*
    val teams = List(24, 1, 13, 17)
    val url = "https://www.balldontlie.io/api/v1/games?seasons[]=2021"
    val response = Http(url).asString.body
*/
val teams = List(24, 1, 13, 17)
val url = "https://www.balldontlie.io/api/v1/games?seasons[]=2021&team_ids[]=1&team_ids[]=13&team_ids[]=17&team_ids[]=24"
val response = Http(url).asString.body
//println(response)


    import spark.implicits._
    val df = spark.read.json(Seq(response).toDS())
    df.printSchema()

    df.write.mode("overwrite").json("match.json")

    df.createOrReplaceTempView("match")
    val match_id = spark.sql("SELECT data.id FROM match")
    match_id.show(15)

    
    //val matchIds = match_id.collect()
    val matchIds = Seq(473359, 473363, 473365, 473422, 473424, 473435, 473464, 473369, 473480, 473471, 473473, 473423, 473441, 473454, 473421, 473457, 473442, 473452, 473439, 473469, 473495, 473463, 473488, 473432, 473410)
    val baseUrl = "https://www.balldontlie.io/api/v1/stats"
    val gameIdsParam = matchIds.map(id => s"game_ids[]=$id").mkString("&")
    val urlstat = s"$baseUrl?$gameIdsParam"
    println(urlstat)
    val response_stat = Http(urlstat).asString.body


    val df_stat = spark.read.json(Seq(response_stat).toDS())
    df_stat.printSchema()
    df_stat.write.mode("overwrite").json("stats.json")
/*
val df = spark.read
  .json(Seq(response).toDS())
  .select(explode($"data").as("game_data"), $"meta")
  .select(
    $"game_data.*",
    struct($"game_data.home_team.id").as("home_team_id"),
    struct($"game_data.visitor_team.id").as("visitor_team_id")
  )
  //.where($"home_team_id".getField("id").cast("int").isin(teams: _*) || $"visitor_team_id".getField("id").cast("int").isin(teams: _*))
  //.where(array_contains(teams, $"home_team_id.id") || array_contains(teams, $"visitor_team_id.id"))
  /*.join(
  teams.toDF("team_id"),
  $"home_team_id.id" === $"team_id" || $"visitor_team_id.id" === $"team_id",
  "inner"
)*/
 /*.join(
  lit(teams).alias("teams"),
  col("home_team_id.id") === col("teams.team_id") || col("visitor_team_id.id") === col("teams.team_id"),
  "inner"
)*/


df.show(truncate = true)

val resultDf = df.select(
  $"data.date",
  $"data.home_team.abbreviation".as("home_team_abbreviation"),
  $"data.home_team.city".as("home_team_city"),
  $"data.home_team.conference".as("home_team_conference"),
  $"data.home_team.division".as("home_team_division"),
  $"data.home_team.full_name".as("home_team_full_name"),
  $"data.home_team.id".as("home_team_id"),
  $"data.home_team.name".as("home_team_name"),
  $"data.home_team_score",
  $"data.visitor_team.abbreviation".as("visitor_team_abbreviation"),
  $"data.visitor_team.city".as("visitor_team_city"),
  $"data.visitor_team.conference".as("visitor_team_conference"),
  $"data.visitor_team.division".as("visitor_team_division"),
  $"data.visitor_team.full_name".as("visitor_team_full_name"),
  $"data.visitor_team.id".as("visitor_team_id"),
  $"data.visitor_team.name".as("visitor_team_name"),
  $"data.visitor_team_score"
)

df.printSchema()

resultDf.write
  .option("header", "true")
  .mode("overwrite")
  .csv("result.csv")
*/
  }
}
