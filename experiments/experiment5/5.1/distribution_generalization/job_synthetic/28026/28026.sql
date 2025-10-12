SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.production_year  >  1985 AND mc.company_id  <  83676 AND mc.company_type_id  >  1 AND mi.info_type_id  >  18