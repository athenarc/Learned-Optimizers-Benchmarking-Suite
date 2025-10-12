SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.production_year  <  1976 AND mc.company_id  <  1705 AND mc.company_type_id  <  2