SELECT COUNT(*)
FROM title t, movie_companies mc, movie_keyword mk
WHERE t.id = mc.movie_id AND t.id = mk.movie_id AND t.kind_id  <  7 AND mc.company_id  =  55215 AND mc.company_type_id  =  2