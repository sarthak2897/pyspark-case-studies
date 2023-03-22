import pyspark.sql.functions
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('Bundesliga_case_study').getOrCreate()

#Who are the winners of the D1 division in the Germany Football Association (Bundesliga) in the last decade?

matches_df = spark.read.option('header','true').option('inferSchema','true').csv('Data\\Matches.csv')

filter_df = matches_df.filter((col('Div') == 'D1') & (col('Season') >= 2000) & (col('Season') <= 2010))\
    .withColumnRenamed('FTHG','HomeTeamGoals').withColumnRenamed('FTAG','AwayTeamGoals').withColumnRenamed('FTR','FullTimeResult') \
    .withColumn('HomeTeamWin', when(col('FullTimeResult') == 'H', 1).otherwise(0)) \
    .withColumn('AwayTeamWin', when(col('FullTimeResult') == 'A', 1).otherwise(0)) \
    .withColumn('GameTie', when(col('FullTimeResult') == 'D', 1).otherwise(0))

#filter_df.show()

def calculateHomeResults(filter_df):
    return filter_df.groupby('Season', 'HomeTeam') \
        .agg(sum('HomeTeamGoals').alias('GoalsScoredAtHome'), sum('AwayTeamGoals')
             .alias('GoalsConcededAtHome'), sum('HomeTeamWin').alias('TotalHomeWins'),
             sum('AwayTeamWin').alias('TotalHomeLosses'), sum('GameTie').alias('TotalHomeTies')) \
        .withColumnRenamed('HomeTeam', 'Team')

def calculateAwayResults(filter_df):
    return filter_df.groupby('Season', 'AwayTeam') \
        .agg(sum('AwayTeamGoals').alias('GoalsScoredAtAway'), sum('HomeTeamGoals')
             .alias('GoalsConcededAtAway'), sum('AwayTeamWin').alias('TotalAwayWins')
             , sum('HomeTeamWin').alias('TotalAwayLosses'), sum('GameTie').alias('TotalAwayTies')) \
        .withColumnRenamed('AwayTeam', 'Team')

def calculateTotalResultsBySeason(homeResults_df,awayResults_df):
    window = Window.partitionBy('Season').orderBy(col('TotalPoints').desc(), col('GoalDifference').desc())
    return homeResults_df.join(awayResults_df, on=['Season', 'Team']) \
        .withColumn('TotalGoalsScored', col('GoalsScoredAtHome') + col('GoalsScoredAtAway')) \
        .withColumn('TotalGoalsConceded', col('GoalsConcededAtHome') + col('GoalsConcededAtAway')) \
        .withColumn('TotalWins', col('TotalHomeWins') + col('TotalAwayWins')) \
        .withColumn('TotalLosses', col('TotalHomeLosses') + col('TotalAwayLosses')) \
        .withColumn('TotalTies', col('TotalHomeTies') + col('TotalAwayTies')) \
        .withColumn('TotalPoints',col('TotalWins') * 3 + col('TotalTies'))\
        .withColumn('GoalDifference', col('TotalGoalsScored') - col('TotalGoalsConceded')) \
        .withColumn('WinPercent',
                    round((col('TotalWins') * 100) / (col('TotalWins') + col('TotalLosses') + col('TotalTies')), 2)) \
        .withColumn('Rank', rank().over(window)) \
        .select('Season', 'Team', 'TotalGoalsScored', 'TotalGoalsConceded', 'TotalWins', 'TotalLosses','TotalTies','TotalPoints',
                'GoalDifference', 'WinPercent', 'Rank')

homeResults_df = calculateHomeResults(filter_df)

awayResults_df = calculateAwayResults(filter_df)

results_df = calculateTotalResultsBySeason(homeResults_df, awayResults_df)


topRankedTeamsBySeason_df = results_df.filter(col('Rank') == 1).orderBy('Season')
#topRankedTeamsBySeason_df.show()


result_per_season_df = results_df.filter(col('Season') == 2009).orderBy('Rank')
#result_per_season_df.show()

bestTeamOfDecade = topRankedTeamsBySeason_df.groupby('Team').agg(count('Team').alias('TotalCount'))\
    .orderBy(col('TotalCount').desc()).select('Team').first()

#print('Team of the decade : '+ bestTeamOfDecade[0])

#Which teams have been relegated in the past 10 years?

year_df = matches_df.filter((col('Div') == 'D1') & (col('Season') >= 2000) & (col('Season') <= 2016))\
    .withColumnRenamed('FTHG','HomeTeamGoals').withColumnRenamed('FTAG','AwayTeamGoals').withColumnRenamed('FTR','FullTimeResult') \
    .withColumn('HomeTeamWin', when(col('FullTimeResult') == 'H', 1).otherwise(0)) \
    .withColumn('AwayTeamWin', when(col('FullTimeResult') == 'A', 1).otherwise(0)) \
    .withColumn('GameTie', when(col('FullTimeResult') == 'D', 1).otherwise(0))

homeResults_df1 = calculateHomeResults(year_df)
awayResults_df1 = calculateAwayResults(year_df)
results_df1 = calculateTotalResultsBySeason(homeResults_df1,awayResults_df1)
totalTeams = int(results_df1.groupby('Season').agg(max('Rank').alias('max_rank')).select('max_rank').first()[0])

relegatedTeamsPerSeason = results_df1.filter(col('Rank') >= (totalTeams - 1)).orderBy('Season','Rank')
#relegatedTeamsPerSeason.show(truncate=False)

#Does Oktoberfest affect Perofrmance of Bundesliga

oktoberfest_df = matches_df.groupby('Season',month(col('Date')).alias('month_num')).agg(sum('FTHG'),sum('FTAG'))\
    .withColumn('TotalGoalsScored',col('sum(FTHG)') + col('sum(FTAG)'))\
    .drop('FTHG','FTAG').orderBy('Season','month_num')
#oktoberfest_df.show(truncate=False)

month_df = oktoberfest_df.filter(col('Season') == 2013).toPandas()['month_num']
fig, ax = plt.subplots(figsize =(12, 4))
ax.hist(month_df)
#plt.show()

#Which season of bundesliga was the most competitive in the last decade?
#results_df1.show()

first_df = results_df1.filter(col('Rank') == 1).alias('first')
second_df = results_df1.filter(col('Rank') == 2).alias('second')
third_df = results_df1.filter(col('Rank') == 3).alias('third')
fourth_df = results_df1.filter(col('Rank') == 4).alias('fourth')

mostCompetitiveSeason_df = first_df.join(second_df,on = 'Season')\
    .join(third_df,on = 'Season').join(fourth_df,on = 'Season')\
    .withColumn('1_2_diff',col('first.TotalPoints') - col('second.TotalPoints'))\
    .withColumn('1_3_diff',col('first.TotalPoints') - col('third.TotalPoints'))\
    .withColumn('1_4_diff',col('first.TotalPoints') - col('fourth.TotalPoints'))\
    .withColumn('winner',col('first.Team'))\
    .select('Season','first.Team','1_2_diff','1_3_diff','1_4_diff')\
    .orderBy('1_2_diff','1_3_diff','1_4_diff')
mostCompetitiveSeason = mostCompetitiveSeason_df.first()
#print('The most competitive season was in '+ str(mostCompetitiveSeason[0]) + ' won by '+mostCompetitiveSeason[1])

def monthName(month_num):
    if(month_num == 1):
        return 'January'
    elif(month_num == 2):
        return 'February'
    elif month_num == 3:
        return 'March'
    elif month_num == 4:
        return 'April'
    elif month_num == 5:
        return 'May'
    elif month_num == 6:
        return 'June'
    elif (month_num == 7):
        return 'July'
    elif month_num == 8:
        return 'August'
    elif month_num == 9:
        return 'September'
    elif (month_num == 10):
        return 'October'
    elif (month_num == 11):
        return 'November'
    elif (month_num == 12):
        return 'December'

month_udf = udf(monthName)
#What's the best month to watch Bundesliga? - based on more goals per match and lesser ties
window1 = Window.partitionBy('Season').orderBy(col('TotalGoalsScored').desc())
bestMonth_df = year_df.withColumn('month_num',month('Date')).withColumn('month',month_udf(col('month_num')))\
    .groupby('Season','month','month_num').agg(sum('HomeTeamGoals'),sum('AwayTeamGoals'))\
    .withColumn('TotalGoalsScored',col('sum(HomeTeamGoals)') + col('sum(AwayTeamGoals)'))\
    .withColumn('Rank',rank().over(window1)).filter(col('Rank') == 1)\
    .groupby('month').agg(count('month')).orderBy(col('count(month)').desc()).select('month').first()

print('Best month to watch Bundesliga as data for past 16 years is '+bestMonth_df[0])






