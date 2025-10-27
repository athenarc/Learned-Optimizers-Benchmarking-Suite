SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an, char_name AS chn, cast_info AS ci, company_name AS cn, info_type AS it, movie_companies AS mc, movie_info AS mi, name AS n, role_type AS rt, title AS t
WHERE ci.note in ( '(voice)','(producer)','(producer)','(executive producer)','(collaborating writer)','(credit only)','(associate producer)','(production assistant)','(uncredited) (voice)','(talent coordinator) (as Jess Cole)' )
    AND cn.country_code = '[us]'
    AND it.info = 'LD year'
    AND mi.info is not null
    AND ( mi.info like '%%'
    OR mi.info like '%%' )
    AND n.gender = 'm'
    AND n.name like '%%'
    AND rt.role = 'actor'
    AND t.production_year > 1948
    AND t.id = mi.movie_id
    AND t.id = mc.movie_id
    AND t.id = ci.movie_id
    AND mc.movie_id = ci.movie_id
    AND mc.movie_id = mi.movie_id
    AND mi.movie_id = ci.movie_id
    AND cn.id = mc.company_id
    AND it.id = mi.info_type_id
    AND n.id = ci.person_id
    AND rt.id = ci.role_id
    AND n.id = an.person_id
    AND ci.person_id = an.person_id
    AND chn.id = ci.person_role_id;;
