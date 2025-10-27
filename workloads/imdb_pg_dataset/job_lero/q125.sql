SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
FROM complete_cast AS cc, comp_cast_type AS cct1, comp_cast_type AS cct2, company_name AS cn, company_type AS ct, keyword AS k, link_type AS lt, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, movie_link AS ml, title AS t
WHERE cct1.kind in ('cast','cast','complete+verified','cast','complete','complete+verified')
    AND cct2.kind = 'complete+verified'
    AND cn.country_code != '[in]'
    AND ( cn.name LIKE '%%'
    OR cn.name LIKE '%%' )
    AND ct.kind = 'distributors'
    AND k.keyword = 'bad-coffee'
    AND lt.link LIKE '%in o%'
    AND mc.note IS NULL
    AND mi.info IN ('UK:3','RAT:2.35 : 1','Documentary','Japanese','PFM:35 mm','English')
    AND t.production_year between 1962
    AND 1975
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
