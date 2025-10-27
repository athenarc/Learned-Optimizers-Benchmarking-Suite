SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND mc.company_id  <  215762 AND ci.person_id  <  3749082 AND ci.role_id  =  3