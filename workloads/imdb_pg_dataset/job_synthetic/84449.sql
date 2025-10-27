SELECT COUNT(*)
FROM title t, movie_info mi
WHERE t.id = mi.movie_id AND t.kind_id  >  2 AND mi.info_type_id  =  5