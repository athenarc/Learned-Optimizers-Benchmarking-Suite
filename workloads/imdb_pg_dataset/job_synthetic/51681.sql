SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.kind_id  =  6 AND t.production_year  >  1996 AND ci.person_id  >  3476663 AND ci.role_id  >  1