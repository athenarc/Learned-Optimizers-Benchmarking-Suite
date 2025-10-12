SELECT COUNT(*)
FROM title t, movie_info mi
WHERE t.id = mi.movie_id AND mi.info_type_id  =  87