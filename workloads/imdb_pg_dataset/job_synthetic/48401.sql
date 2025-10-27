SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND ci.person_id  =  3255504 AND mi.info_type_id  =  18