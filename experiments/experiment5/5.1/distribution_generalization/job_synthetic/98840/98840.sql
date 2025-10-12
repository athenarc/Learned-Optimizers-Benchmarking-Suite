SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  >  2008 AND ci.person_id  <  626874 AND mi.info_type_id  <  3