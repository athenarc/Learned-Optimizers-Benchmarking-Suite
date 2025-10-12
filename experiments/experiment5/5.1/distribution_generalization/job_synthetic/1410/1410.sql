SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND t.kind_id  =  7 AND mc.company_id  >  2042 AND mc.company_type_id  <  2 AND ci.person_id  >  3064096 AND ci.role_id  >  4