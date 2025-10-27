SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  =  1942 AND ci.person_id  <  187962 AND mi.info_type_id  >  4