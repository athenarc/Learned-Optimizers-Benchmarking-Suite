SELECT COUNT(*)
FROM title t, movie_companies mc, cast_info ci
WHERE t.id = mc.movie_id AND t.id = ci.movie_id AND mc.company_id  =  192173 AND ci.person_id  >  2793219 AND ci.role_id  <  10