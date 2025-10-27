SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  <  1987 AND mc.company_id  <  3139 AND mc.company_type_id  <  2