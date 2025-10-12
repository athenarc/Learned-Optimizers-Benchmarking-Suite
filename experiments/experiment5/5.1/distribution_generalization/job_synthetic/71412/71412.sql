SELECT COUNT(*)
FROM title t, cast_info ci, movie_info mi
WHERE t.id = ci.movie_id AND t.id = mi.movie_id AND t.kind_id  >  3 AND ci.person_id  =  623300 AND mi.info_type_id  =  4