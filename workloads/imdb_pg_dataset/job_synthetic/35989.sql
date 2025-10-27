SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  >  2 AND t.production_year  =  2009 AND ci.person_id  <  2738297