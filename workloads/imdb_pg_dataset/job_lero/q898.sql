SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
FROM complete_cast AS cc, comp_cast_type AS cct1, comp_cast_type AS cct2, company_name AS cn, company_type AS ct, keyword AS k, link_type AS lt, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, movie_link AS ml, title AS t
WHERE cct1.kind in ('crew','complete','complete','complete','complete','crew','cast','complete','complete+verified','cast')
    AND cct2.kind = 'complete'
    AND cn.country_code != '[mx]'
    AND ( cn.name LIKE '%i%'
    OR cn.name LIKE '%i%' )
    AND ct.kind = 'production companies'
    AND k.keyword = 'racial-diversity'
    AND lt.link LIKE '%e%'
    AND mc.note IS NULL
    AND mi.info IN ('USA:28 March 2010','Sport','Color','Canada','Game-Show','Nettlefold Studios, Walton-on-Thames, Surrey, England, UK','USA:1987','OFM:35 mm','RAT:1.33 : 1','USA')
    AND t.production_year between 1990
    AND 2010
    AND lt.id = ml.link_type_id
    AND ml.movie_id = t.id
    AND t.id = mk.movie_id
    AND mk.keyword_id = k.id
    AND t.id = mc.movie_id
    AND mc.company_type_id = ct.id
    AND mc.company_id = cn.id
    AND mi.movie_id = t.id
    AND t.id = cc.movie_id
    AND cct1.id = cc.subject_id
    AND cct2.id = cc.status_id
    AND ml.movie_id = mk.movie_id
    AND ml.movie_id = mc.movie_id
    AND mk.movie_id = mc.movie_id
    AND ml.movie_id = mi.movie_id
    AND mk.movie_id = mi.movie_id
    AND mc.movie_id = mi.movie_id
    AND ml.movie_id = cc.movie_id
    AND mk.movie_id = cc.movie_id
    AND mc.movie_id = cc.movie_id
    AND mi.movie_id = cc.movie_id;;
