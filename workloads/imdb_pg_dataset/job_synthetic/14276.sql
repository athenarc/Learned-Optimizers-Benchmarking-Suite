SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND mc.company_id  <  88171 AND mc.company_type_id  =  2