SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  <  1953 AND mc.company_id  >  71348