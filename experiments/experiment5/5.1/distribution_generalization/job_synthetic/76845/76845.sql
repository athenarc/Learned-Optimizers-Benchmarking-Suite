SELECT COUNT(*)
FROM title t, movie_info mi
WHERE t.id = mi.movie_id AND t.production_year  >  1917 AND mi.info_type_id  <  16