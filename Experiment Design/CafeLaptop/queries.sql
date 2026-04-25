WITH import AS (
SELECT *
FROM read_csv('https://raw.githubusercontent.com/noahgomz/Growth_Analytics_Portfolio/refs/heads/master/Experiment%20Design/CafeLaptop/CafeLaptop_Campaign_AB_1_raw.csv')
),
clean AS (
SELECT Campaign_name, Ad_name, Day, Age, Gender, Reach, Impressions, Results AS LPViews, "Link clicks" as Link_clicks, "Gross_impressions_(includes_invalid_impressions_from_non-human_traffic)" as Gross_Impressions, Post_engagements, Post_comments, Post_reactions, Post_saves
FROM import
--WHERE Day > '2026-04-08'

),
agebrkdwn AS (                                -- FOR SANITY CHECK
SELECT Campaign_name, Ad_name, Age, SUM(Reach), SUM(LPViews),
ROUND(SUM(LPViews) / SUM(Reach),4) as convRt
FROM clean
GROUP BY Campaign_name, Ad_name, Age
ORDER BY Ad_name, convRt DESC, Age
),
genderbrkdwn AS (                               -- FOR SANITY CHECK
SELECT Campaign_name, Ad_name, Gender, SUM(Reach), SUM(LPViews),
ROUND(SUM(LPViews) / SUM(Reach),4) as convRt
FROM clean
GROUP BY Campaign_name, Ad_name, Gender
ORDER BY Ad_name, convRt DESC, Gender
)

SELECT Ad_name, SUM(Impressions), SUM(Reach),
    CASE
        WHEN Ad_name = 'Benefit Led' THEN 4411
        WHEN Ad_name = 'Problem Led' THEN 4126
    END as "nonAdditiveReach[MetaSUM]", -- Reach is non-additive in raw data because users may be counted >1 per breakdown but are not duplicated in the aggregate
    SUM(LPViews) 
FROM clean
GROUP BY Ad_name

