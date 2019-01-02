import matplotlib.pyplot as plt
import csv
import seaborn as sns
import pandas as pd
import numpy as np


x = []
y = []

df = pd.read_csv('/Users/jf250049/Documents/Archive/SparkScala/src/main/resources/movieRatingsSheet.csv/avgMovieRatings.csv')
# sns.kdeplot(df['avgRating'], shade=True)
# sns.kdeplot(df['countRating'] < 20, shade=True)
# sns.kdeplot(np.log(df['countRating']), shade=True)
# plt.xlim(0, 750)
# plt.show()

# df.boxplot(column=['countRating'], grid=False)
# plt.plot(df['avgRating'], df['countRating'], 'ro')
# plt.ylim(0, 100)
# sns.distplot(df["countRating"], bins=5)

# plt.show()

# plt.bar(df["movieId"], df["countRating"])
# plt.show()
df['countRating'] = df.countRating.astype('int')
df['avgRating'] = df.avgRating.astype('double')

bins = [0, 10, 100, 1000, 5000, 10000, 50000]

bins_avgRating = [0, 1, 2, 3, 4, 5]


# sns.distplot( df.query("countRating < 50")["countRating"], bins=20)
# plt.bar(new_df['avgRating'], new_df['countRating'], alpha=0.3)
# plt.xticks(new_df['avgRating'], (1, 2, 3, 4, 5))
# plt.show()

new_df  = df.query("countRating > 10")
df = new_df.groupby(pd.cut(new_df['avgRating'], bins=bins_avgRating)).countRating.count()
ax = df.plot(kind='bar',
        title="Distribution of amount of ratings over the 50,000 movies dataset",
        x="Rating amount thresholds (bins)",
        y="Number of ratings", rot=0)

ax.set_xlabel("Rating amount thresholds (bins)")
ax.set_ylabel("Number of ratings")
plt.show()



# ---- avg rating on the aggregarted sum of movies

# df = df.groupby(pd.cut(df['countRating'], bins=bins)).avgRating.mean()
# # df = df.groupby(pd.cut(df['countRating'], bins=bins)).apply(lambda x: x["avgRating"])
# # print(df.head())
#
# ax = df.plot(kind='bar', title="Distribution of average ratings over the 50,000 movies", rot=0)
#
# ax.set_xlabel("Average rating score (bins)")
# ax.set_ylabel("Number of ratings")
# plt.show()






# with open('/Users/jf250049/Documents/Archive/SparkScala/src/main/resources/movieRatingsSheet.csv/avgMovieRatings.csv', 'r') as csvfile:
#     plots = csv.reader(csvfile, delimiter=',')
#     next(plots)
#     for row in plots:
#         x.append(float(row[1]))
#         y.append(int(row[3]))
#
#
# # plt.hist(y, bins=10000, density=False)
# # plt.xlim(0, 3000)
# # plt.show()
#
# # plt.imshow(x, cmap='hot', interpolation='nearest')
# # plt.fill_between(x, y, color="skyblue", alpha=0.3)
#
#
#
# # Same, but add a stronger line on top (edge)
# # plt.fill_between(x, y, color="skyblue", alpha=0.2)
# # plt.plot(x, y,color="Slateblue", alpha=0.6)
# #
# plt.plot(x,y, label='Rating value vs rating count', alpha=0.3)
# plt.ylabel('Number of ratings')
# plt.xlabel('Rating')
# plt.legend()
# # plt.ylim(0, 20000)
# plt.show()
