SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.kind_id  <  7 AND mc.company_id  =  14923 AND mi.info_type_id  >  105