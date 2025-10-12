SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.kind_id  <  3 AND mc.company_id  <  47506 AND mi.info_type_id  =  16