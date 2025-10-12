SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  =  1934 AND ci.person_id  >  356575 AND ci.role_id  >  4