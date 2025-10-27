SELECT COUNT(*)
FROM title t, cast_info ci, movie_keyword mk
WHERE t.id = ci.movie_id AND t.id = mk.movie_id AND t.production_year  >  1924 AND ci.person_id  >  3522686 AND ci.role_id  >  3