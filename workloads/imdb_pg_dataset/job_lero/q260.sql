SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_nerdy_internet_movie
FROM complete_cast AS cc, comp_cast_type AS cct1, company_name AS cn, company_type AS ct, info_type AS it1, keyword AS k, kind_type AS kt, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, title AS t
WHERE cct1.kind = 'complete+verified'
    AND cn.country_code = '[fr]'
    AND it1.info = 'locations'
    AND k.keyword in ('grocery','bedtime-prayer','dune-buggy','train-robbery','wind')
    AND kt.kind in ('tv mini series','tv movie','tv mini series','tv movie','tv series','tv series','movie','tv movie')
    AND mi.note like '%io%'
    AND mi.info like '%5%'
    AND t.production_year > 1969
    AND kt.id = t.kind_id
    AND t.id = mi.movie_id
    AND t.id = mk.movie_id
    AND t.id = mc.movie_id
    AND t.id = cc.movie_id
    AND mk.movie_id = mi.movie_id
    AND mk.movie_id = mc.movie_id
    AND mk.movie_id = cc.movie_id
    AND mi.movie_id = mc.movie_id
    AND mi.movie_id = cc.movie_id
    AND mc.movie_id = cc.movie_id
    AND k.id = mk.keyword_id
    AND it1.id = mi.info_type_id
    AND cn.id = mc.company_id
    AND ct.id = mc.company_type_id
    AND cct1.id = cc.status_id;;
