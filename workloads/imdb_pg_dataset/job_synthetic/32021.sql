SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND mc.company_id  <  13157 AND mc.company_type_id  >  1