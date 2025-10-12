SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info mi
WHERE t.id = mc.movie_id AND t.id = mi.movie_id AND t.kind_id  >  1 AND mc.company_id  >  71738 AND mi.info_type_id  =  16