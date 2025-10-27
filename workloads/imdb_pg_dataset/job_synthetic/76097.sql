SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND mc.company_id  <  194477 AND mc.company_type_id  =  2