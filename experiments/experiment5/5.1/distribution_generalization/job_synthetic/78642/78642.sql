SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  >  2012 AND ci.person_id  <  2805385 AND mi.info_type_id  =  18