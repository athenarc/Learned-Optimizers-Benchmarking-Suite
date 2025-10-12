SELECT COUNT(*)
FROM title t, cast_info ci
WHERE t.id = ci.movie_id AND t.production_year  =  1925 AND ci.person_id  <  1845068 AND ci.role_id  =  1