SELECT COUNT(*)
FROM title t, movie_info mi
WHERE t.id = mi.movie_id AND t.kind_id  <  2 AND t.production_year  >  1983 AND mi.info_type_id  >  1