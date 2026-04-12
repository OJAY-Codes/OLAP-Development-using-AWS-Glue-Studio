CREATE TABLE IF NOT EXISTS Average_Skill_Wage
  WITH (format='PARQUET', external_location='s3://myfirst-bucket-126606499301-eu-north-1-an/Analytics/') AS
SELECT skill.primary_skill, Round(avg("hourly_rate__usd_#4"), 0) as Average_wage,count("hourly_rate__usd_#4") as Number, sum("hourly_rate__usd_#4") as Rate
FROM facts_table
JOIN freelancer 
ON facts_table."freelancer_id#0" = freelancer.freelancer_id
JOIN skill
ON facts_table."skill_id#1" = skill.skill_id
WHERE freelancer.age > 20 AND freelancer.age < 60
GROUP BY skill.primary_skill
ORDER BY Average_wage DESC;