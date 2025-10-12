SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.production_year  =  1998 AND mc.company_id  >  16854 AND mc.company_type_id  =  1