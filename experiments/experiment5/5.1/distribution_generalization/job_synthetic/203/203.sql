SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id AND t.kind_id  >  1 AND t.production_year  <  1986 AND mc.company_id  <  13251 AND mc.company_type_id  <  2