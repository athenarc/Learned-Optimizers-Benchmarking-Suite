SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info_idx mi_idx
WHERE t.id = mc.movie_id AND t.id = mi_idx.movie_id AND mc.company_id  <  42315 AND mc.company_type_id  =  2 AND mi_idx.info_type_id  >  99