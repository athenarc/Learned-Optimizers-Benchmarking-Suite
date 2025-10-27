select min(n.name), min(t.title)
from info_type as it1,
info_type as it2,
movie_info_idx as mi_idx,
title as t,
cast_info as ci,
name as n,
person_info as pi
WHERE it1.info ILIKE 'rating'
AND it1.id = mi_idx.info_type_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND ci.person_id = n.id
AND n.id = pi.person_id
AND pi.info_type_id = it2.id
AND it2.info ILIKE '%birth%'
AND pi.info ILIKE '%india%'
AND (mi_idx.info ILIKE '8%' OR mi_idx.info ILIKE '9%'
  OR mi_idx.info ILIKE '10%');