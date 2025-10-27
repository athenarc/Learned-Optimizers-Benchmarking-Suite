SELECT COUNT(*)
FROM title t, movie_companies mc
WHERE t.id = mc.movie_id