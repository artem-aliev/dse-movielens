

class SparkExamples extends App {

  // Get the same data we had before into Spark.
  // This is Datastax Spark/Graph integration. It support both TinkerPop and
  // GraphFrames for the same source
  val g: GraphFrame = spark.dseGraph("movielens").cache

  // Simple counts are fast and easy
  // GraphFrames are just pair of vertices and edges dataframes.
  // Thus all dataframe methods could be applied to them
  g.vertices.count
  g.edges.count

  // Vertices dataframe should have 'id' columns.
  g.vertices.show(3)
  // Edges one should have 'src' and 'dst' columns. All three names are hardcoded
  g.edges.show(3)
  // No support for vertex types, thus I added '~label' column to support them.

  // Traversals with GraphFrames are done with "motif finding language"
  // A sequence of patterns in the form of vertex to edge to vertex triplets
  // All patterns can have named variables that are mapped in a result dataframe struct column

  // This is the same request I used before:
  // What are programmers watching
  // But it's longer. 325 vs 136 symbols
  g.find(
    """
    (user)-[occupation_e]->(occupation);
    (user)-[rated_e]->(movie);
    (movie)-[genre_e]->(genre)
    """).filter(
    """
    occupation.name = "programmer"
    and occupation_e.`~label` = "occupation"
    and rated_e.`~label` = "rated"
    and genre_e.`~label` = "genre"
    """).groupBy("genre.name").count().show

  // The sql request to properly normalized database will looks like this
  // 6 joins
  spark.sql(
    """
select genre.name, count(1) from occupation
join occupation_e on occupation.id = occupation_e.dst
join user on user.id = occupation_e.src
join rated_e on user.id = rated_e.src
join movie on movie.id = rated_e.dst
join genre_e on movie.id = genre_e.src
join genre on genre.id = genre_e.dst
where occupation.name = "programmer"
group by genre.name
|""").show

  // GraphFrame dataframes are not normalized, it has only two tables,
  // so the SQL will look more complicated
  // let register vertices and edges as temporary tables
  g.vertices.createOrReplaceTempView("v")
  g.edges.createOrReplaceTempView("e")


  // Now our join will looks a little bit more wordy
  // The logic is still the same: traversing from 'occupation'  to 'genre'
  // The 'where' statement is the the same as graph frame filter.
  // The 'joins' repeat motif finding part.
  // Actually, 'motif finding' is a syntactic sugar for doing these joins
  spark.sql(
    """
select genre.name, count(1) from v occupation
join e occupation_e on occupation.id = occupation_e.dst
join v user on user.id = occupation_e.src
join e rated_e on user.id = rated_e.src
join v movie on movie.id = rated_e.dst
join e genre_e on movie.id = genre_e.src
join v genre on genre.id = genre_e.dst
where occupation.name = "programmer"
and occupation_e.`~label` = "occupation"
and rated_e.`~label` = "rated"
and genre_e.`~label` = "genre"
group by genre.name
|""").show


  // Join with other non-Graph sources
  // Let's imagine we have data source with user data, this could be MySQL, Cassandra, CSV ...
  val names = List((710, "Russ")).toDF("uid", "user_name")
  // We can enrich our graph with the external data
  val v = g.vertices
  val nv = v.join(names, v("_id") === names("uid") and v("~label") === "user", "left")
  val ng = GraphFrame(nv, g.edges)
  // and find that Russ has watched 885 movies!
  ng.find("(user)-[rated_e]->(movie)").filter(
    """
    user.user_name = "Russ"
    and rated_e.`~label` = "rated"
    """).count()


  // Export is simple
  g.vertices.write.save("ml_v")
  g.edges.write.save("ml_e")
  // Import is as simple as loading two df:
  val g2 = Graphframe(spark.sql("select id from v"), (spark.sql("select src, dst from e"))


  // Example of Label Propagation algorithm
  val lp = g.labelPropagation.maxIter(5).run()

}
