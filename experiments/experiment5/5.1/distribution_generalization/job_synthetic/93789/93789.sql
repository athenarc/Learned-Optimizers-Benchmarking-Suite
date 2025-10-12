SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.production_year  >  1959 AND ci.person_id  <  4043192 AND ci.role_id  >  1 AND mi.info_type_id  >  77