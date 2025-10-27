SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND ci.person_id  =  2592067 AND mi.info_type_id  >  8