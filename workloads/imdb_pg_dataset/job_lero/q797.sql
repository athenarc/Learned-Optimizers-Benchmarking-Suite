SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS character_name, MIN(t.title) AS movie
FROM aka_name AS an, char_name AS chn, cast_info AS ci, company_name AS cn, movie_companies AS mc, name AS n, role_type AS rt, title AS t
WHERE ci.note in ( '(voice)','(uncredited)','(location manager)','(executive producer)','(producer)','(written by)','(as Jessica Chapnik)' )
    AND cn.country_code = '[us]'
    AND mc.note is not NULL
    AND ( mc.note like '%(USA)%'
    OR mc.note like '%(worldwide)%' )
    AND n.gender = 'f'
    AND n.name like '%Ang%'
    AND rt.role = 'actress'
    AND t.production_year between 1989
    AND 2005
    AND ci.movie_id = t.id
    AND t.id = mc.movie_id
    AND ci.movie_id = mc.movie_id
    AND mc.company_id = cn.id
    AND ci.role_id = rt.id
    AND n.id = ci.person_id
    AND chn.id = ci.person_role_id
    AND an.person_id = n.id
    AND an.person_id = ci.person_id;;
