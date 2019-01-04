cmd + shift + p -> preview in browser
# Software development rotation 

[Proof of concept time decay](#proof-of-concept-trying-to-weight-ratings-based-on-how-recent-they-are)
### Applying a threshold on the minimum number of ratings a movie should have
```python
df = df.groupby(pd.cut(df['countRating'], bins=bins)).countRating.count()
ax = df.plot(kind='bar',
        title="Distribution of amount of ratings over the 50,000 movies dataset",
        x="Rating amount thresholds (bins)",
        y="Number of ratings", rot=0)

ax.set_xlabel("Rating amount thresholds (bins)")
ax.set_ylabel("Number of ratings")
plt.show()
```

![alt text](/Users/jf250049/Desktop/ratings_distribution.png)

```python
df = df.groupby(pd.cut(df['avgRating'], bins=bins_avgRating)).countRating.count()
ax = df.plot(kind='bar', title="Distribution of average ratings over the 50,000 movies", rot=0)

ax.set_xlabel("Average rating score (bins)")
ax.set_ylabel("Number of ratings")
plt.show()
```

![alt text](/Users/jf250049/Desktop/average_rating_distribution.png)

#### Simple analysis - Understanding the nature of the dataset
###### Irrespective of which movies have which score, the distribution is as follows:
```python
ratings_df = total_ratings.groupby(pd.cut(total_ratings['rating'], bins=bins_avgRating)).rating.count()
ax = ratings_df.plot(kind='bar', title="Distribution of total ratings given to all movies", rot=0)

ax.set_xlabel("Rating score thresholds (bins)")
ax.set_ylabel("Number of ratings")
plt.show()
```
![alt text](/Users/jf250049/Desktop/total_ratings_distribution.png)

#### Weighted average rating of movies
###### When taking the amount of ratings each movie has, the dataset is skewed by the number of movies with very few ratings
```python
weight_df = df_weighted_05.groupby(pd.cut(df_weighted_05['weightedAvg'], bins=bins_weighted_avg_rating)).countRating.count()
ax = weight_df.plot(kind='bar', title="Distribution of weighted average ratings over all movies", rot=0)

ax.set_xlabel("Weighted average rating score (bins)")
ax.set_ylabel("Number of ratings")
plt.show()
```
![alt text](/Users/jf250049/Desktop/weightedAvg.png)

## Determining which movies to invest in
#### Finding out which tags are most relevant
1. Determine the average rating of each movie
2. Select out the movies which are highly appraised
3. Map official, individual tags to movies
4. Understand which tags are most indicative of a good movie
5. Score tags with a percentage weighting for investment

#### Finding out which genres are most relevant
1. Repeat steps 1-2 from above
2. Map individual genres to movies
3. Understand which genre(s) correlate to the highest appraised movies
4. Score genres with a percentage weighting for investment

---

#### Calculating average movie rating

##### Intermediate step 1 - Weight all ratings according to the year in which the rating was done
The calculation is as follows: rating / (current_year + 1 - year_of_rating)
```scala
val ratingsWeighted = ratings.withColumn("year", year($"timestamp"))
                             .withColumn("tsWeighted", $"rating".divide(lit(2019) - $"year"))
```
##### Perform average calculation based on the ratings weighted above
```scala
val movieAvg = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
      .select("movieId", "movieTitle", "tsWeighted")
      .groupBy("movieId")
      .agg(avg("tsWeighted"))
      .join(movies, "movieId")
      .select($"avg(tsWeighted)".alias("avgRating"), $"movieTitle", $"movieId")
```

##### Intermediate step 2 - Calculate the amount of ratings per movie
```scala
val ratingCount = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
      .select("movieId", "movieTitle", "tsWeighted")
      .groupBy("movieId")
      .agg(count("tsWeighted"))
      .join(movies, "movieId")
      .select($"count(tsWeighted)", $"movieTitle", $"movieId")
```
##### Resulting average movie rating calculation - joining both datasets
```scala
val resAvg = movieAvg.join(ratingCount, movie("movieId") === ratingCount("movieId"))
  .select(movieAvg("movieId"),
   		  movieAvg("avgRating"),
   		  movieAvg("movieTitle"), 
   		  ratingCount("count(tsWeighted)").as("countRating"))

```
The result is a table in the following form:

|movieId|          avgRating|          movieTitle|countRating|
|-------|-------------------|--------------------|-----------|
|    148|0.19888012735710248|"Awfully Big Adve...|        374|
|    463|0.19355017605814973|       Guilty as Sin|        432|
|    471| 0.2946686833111162|"Hudsucker Proxy,...|      12308|
|    496|0.18872342470565237|What Happened Was...|        424|
|    833| 0.1958301119951954|    High School High|       1562|


##### Create a dataset to enable statistical analysis on a per tag basis
This was done by joining the above with the official tags dataset (genome tags). There is scope for experimentation here around what tag relevancy score to use as the threshold.
The threshold score dictates the minimum score necessary to consider a tag to be associated to a given movie.
```scala
val tagJoin = genome.as("g")
      .join(resAvg.as("resAvg"), $"g.movieId" === $"resAvg.movieId")
      .select($"g.tagName", $"g.tagId",$"g.relevance", $"g.movieId", $"resAvg.avgRating", $"resAvg.countRating")
      .where($"g.relevance" > 0.5)
```
With the output:

|             tagName|tagId|         relevance|movieId|        avgRating|countRating|
|--------------------|-----|------------------|-------|-----------------|-----------|
|            original|  742|0.7595000000000001|    148|2.907754010695187|        374|
|               story|  971|            0.5085|    148|2.907754010695187|        374|
|               weird| 1104|            0.7395|    148|2.907754010695187|        374|
|         catastrophe|  188|             0.504|    463|2.810185185185185|        432|
|               chase|  195|            0.5065|    463|2.810185185185185|        432|

This allows the grouping by tag, to eventually reach an average rating per tag based on the previous average ratings of each movie associated with that tag.

##### Calculating weighted average rating by taking the amount of ratings a movie has into consideration

This is done to avoid situations where movies with one rating of 5* has the same weight as a movie with 2500 ratings but an average of 3.4 stars.
This is achieved on an individual movie basis like so: **log(movieRatingCount) * movieAvgRating**
```scala
val weightedCalc = tagJoin.withColumn("weightedAvg", log("resAvg.countRating") * tagJoin("resAvg.avgRating"))
```

|             tagName|tagId|         relevance|movieId|        avgRating|countRating|       weightedAvg|
|--------------------|-----|------------------|-------|-----------------|-----------|------------------|
|            original|  742|0.7595000000000001|    148|2.907754010695187|        374| 17.22627855531632|
|               story|  971|            0.5085|    148|2.907754010695187|        374| 17.22627855531632|
|               weird| 1104|            0.7395|    148|2.907754010695187|        374| 17.22627855531632|
|         catastrophe|  188|             0.504|    463|2.810185185185185|        432|17.053399685482294|
|               chase|  195|            0.5065|    463|2.810185185185185|        432|17.053399685482294|
|               1930s|    5|            0.6645|    471|3.652908677283068|      12308| 34.40311122444177|

##### Discovering the highest average rating per tag
```scala
weightedCalc.select("tagName", "weightedAvg").groupBy("tagName").agg(avg("weightedAvg")).orderBy(desc("avg(weightedAvg)")).show()
```
The group by here is done on the tagName column for visualisation, in production tagId is used as the join column with the above table later.

|                      tagName|             Score|
|-----------------------------|------------------|
|awesome                      |10.187229299732897|
|emma watson                  |10.033330948791788|
|harry potter                 |9.997894171177746 |
|marvel                       |9.972005921183468 |
|comic book adaption          |9.563848329228472 |
|watch the credits            |9.200371808185581 |
|studio ghibli                |9.065309510109511 |

In comparison, the following is the result **without** timestamp weighting on the ratings

|                                 tagName|             Score|
|----------------------------------------|------------------|
|brilliant                               |45.447445582123926|
|saturn award (best science fiction film)|37.43401883865303 |
|afi 100                                 |36.19171631629124 |
|awesome                                 |35.05258692401865 |
|oscar (best sound)                      |34.81434890301194 |
|tarantino                               |33.78763886119306 |
|oscar (best music - original score)     |33.54690699031136 |
|oscar (best editing)                    |33.10239591343306 |

#### Choosing a threshold tag score of 7
![alt text](/Users/jf250049/Desktop/tagRatings.png)
There are a small number of tags above the score of 8, the graph starts to level out around the score of 7, which seems like a suitable threshold.
We are looking for outstanding movies, below 7 until roughly 3.8 tags tend to have fairly similar scores as the curve stabilises.

#### Determining allocation percentage of tags
Firstly, select out the weighted scores above the chosen threshold:
```scala
val tagAvgRatings = weightedCalc.select("tagId",  "weightedAvg")
  .groupBy("tagId").agg(avg("weightedAvg")).select("tagId", "avg(weightedAvg)")
  .where($"avg(weightedAvg)" >= 7.0).orderBy(desc("avg(weightedAvg)"))
```

Scores are then scaled based on percentage difference, therefore the higher values (furthest away from the threshold) are weighted higher
```scala
val decisionWeighting = tagAvgRatings.select("avg(weightedAvg)", "tagId")
                        .withColumn("decisionScore", ($"avg(weightedAvg)" - lit(7.0)).divide(lit(7.0)) * lit(100))
                        .select($"decisionScore", $"avg(weightedAvg)", $"tagId")
```

Finally, these scores are standardised to output them as a percentage of the total scores which is used as the allocation percentage for the client
```scala
val decisionSum = decisionWeighting.agg(sum("decisionScore")).first().getDouble(0)
val decisionScore = decisionWeighting.withColumn("allocation", $"decisionScore".divide(lit(decisionSum)) * lit(100))
```
Of course, the higher the threshold, the better the allocation will be as it will be more picky with percentage difference

#### Establishing reliability
Standard deviation will tells the spread of our ratings, we only want movies which are rated consistently high, so the aim is to find tags with the highest mean but lowest standard deviation
```scala
val stdDeviation = movies.join(ratingsWeighted, movies("movieId") === ratingsWeighted("itemId"))
  .select("movieId", "movieTitle", "tsWeighted")
  .groupBy("movieId")
  .agg(stddev_pop("tsWeighted"))
  .join(movies, "movieId")
  .select($"stddev_pop(tsWeighted)".alias("stdDeviation"), $"movieTitle", $"movieId")

 val finalRes = resAvg.join(stdDeviation, resAvg("movieId") === stdDeviation("movieId"))
 				.select(avg1("movieId"), avg1("avgRating"), avg1("movieTitle"), resAvg("countRating"), stdDeviation("stdDeviation"))
 				.withColumn("CV", $"stdDeviation".divide($"avgRating"))
```

![alt text](/Users/jf250049/Desktop/standardised.png)

Choosing a strict threshold of 2.5 here results in

|                                 tagName|             Score|             Allocation|
|----------------------------------------|------------------|-----------------------|
|brilliant                               |5.319425002823082 |     11.782570924073088|
|marx brothers 							 |5.026661336350778 |     10.559091434195679|
|truman capote                           |5.019494620272776 |     10.529141234989387|
|hannibal lecter                         |4.7515702456222275|     9.409466853352319 |
|miyazaki                      		     |4.624031829649795 |     8.876475044655706 |
|aardman                                 |3.5395173956589603|     4.3442146639457935|
|small town                              |3.451586399493341 |     3.9767449856573442|
|penguins                                |3.438570352512136 |     3.922350030461318 |


##### Discovering the highest average rating per genre
A map is created of the form (genreName -> List of movie IDs with that genre), parquet files are created from this and then unioned together to result in one dataframe with average ratings, movie Ids and genres:
```scala
val genreList = List("Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
     				 "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller",
      				 "War", "Western")

val genreMap = genreList.map(s => movies
      .filter(array_contains($"genres", s))
      .select("movieId").map(r => r.getInt(0)).collect.toList).zipWithIndex.map(t => genreList(t._2) -> t._1).toMap

for ((k,v) <- newList) avgMovieRatings.select("*").where($"movieId".isin(v:_*)).withColumn("genre", lit(k)).write.mode(SaveMode.Overwrite).format("parquet").save(f"src/main/resources/$k%s.parquet")
...
val genreRatings = genreDf.groupBy("genre").agg(avg("avgRating"))
```

|      genre|    avg(avgRating)|        allocation|
|-----------|------------------|------------------|
|  Animation|1.6132988326262307| 8.962771292367949|
|Documentary|1.4741410120212364|  8.18967228900687|
|   Children| 1.396329385919741| 7.757385477331895|
|    Fantasy| 1.291426461385756|  7.17459145214309|
|     Comedy|1.2447117022416898| 6.915065012453832|
|      Drama|1.2335428274643823| 6.853015708135457|
|    Romance|1.2301248822546706|6.8340271236370596|
|    Mystery|1.2026892802559568| 6.681607112533093|
|     Action| 1.200045595393877| 6.666919974410428|
|     Sci-Fi| 1.194815607346662| 6.637864485259233|
|   Thriller|1.1697242457123924| 6.498468031735513|
|  Adventure|1.1484714234825373| 6.380396797125207|
|      Crime|1.1445769369801817| 6.358760761001009|
|     Horror|1.1179703246322203|  6.21094624795678|
|        War|1.1008587897912252|6.1158821655068065|
|    Western|1.0112288134883851| 5.617937852713251|
|  Film-Noir|0.8982966099322304| 4.990536721845724|
|    Musical|0.8017342259033022| 4.454079032796124|



![alt text](/Users/jf250049/Desktop/genreScore.png)
---
#### Proof of concept - Trying to weight ratings based on how recent they are
###### This was done by implementing Reddit's old 'hotness' algorithm as per the below Python code in Scala - more info available at https://medium.com/hacking-and-gonzo/how-reddit-ranking-algorithms-work-ef111e33d0d9

```python
def hotness(track)
    s = track.playedCount
    s = s + 2*track.downloadCount
    s = s + 3*track.likeCount
    s = s + 4*track.favCount
    baseScore = log(max(s,1))

    timeDiff = (now - track.uploaded).toWeeks

    if(timeDiff > 1)
        x = timeDiff - 1
        baseScore = baseScore * exp(-8*x*x)

    return baseScore
```

```scala
val formattedTimes = ratings
  .withColumn("latest", lit(getLatestTimestamp))
  .select($"rating", $"itemId",
    round(datediff($"latest", $"timestamp").divide(lit(7))).as[Double].as("Weeks"))
  .withColumn("Weeks", when($"Weeks" > 1, $"Weeks" - 1))
  .withColumn("loggedRating", log($"rating"))
  .withColumn("loggedRating", when($"loggedRating" < 1, 1).otherwise(col("loggedRating")))
  .filter($"timeDifference" > 1).map(row => $"timeDifference").map(row => row - 1)


val newDs = formattedTimes.withColumn("weightedRating", $"loggedRating" * exp(lit(-8) * $"Weeks".multiply($"Weeks")))
```

![alt text](/Users/jf250049/Desktop/weighted_timestamp.png)

compare the averag eof a genre to the distribution, also for tags
look at the difference in weighted ratings after threshold is changed (currently its 0.5)