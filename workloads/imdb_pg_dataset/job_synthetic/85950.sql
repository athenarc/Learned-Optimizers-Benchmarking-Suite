SELECT COUNT(*)
FROM title t, movie_companies mc, movie_info_idx mi_idx
WHERE t.id = mc.movie_id AND t.id = mi_idx.movie_id AND t.kind_id  >  1 AND t.production_year  <  2002 AND mc.company_id  <  11991 AND mc.company_type_id  >  1