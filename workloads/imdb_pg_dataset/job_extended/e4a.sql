SELECT MIN(mi.info) AS release_date, MIN(mi_idx.info) AS rating, MIN(t.title) AS hongkong_movie 
FROM company_name cn, company_type ct, info_type it, kind_type kt, movie_companies mc, movie_info mi, movie_info_idx mi_idx, title t 
WHERE cn.country_code ='[hk]' AND ct.kind ='production companies' AND it.info ='rating' AND kt.kind ='movie' 
AND mi.movie_id = t.id AND kt.id = t.kind_id AND mc.movie_id = t.id AND cn.id = mc.company_id AND ct.id = mc.company_type_id 
AND mi_idx.movie_id = t.id AND it.id = mi_idx.info_type_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mi_idx.movie_id = mc.movie_id;
