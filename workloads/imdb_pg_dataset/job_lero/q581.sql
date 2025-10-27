SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie
FROM cast_info AS ci, keyword AS k, movie_keyword AS mk, name AS n, title AS t
WHERE k.keyword in ( 'guilt','arm-in-sling','feminist','based-on-journal','laundry-room','pretending','murder-of-king' )
    AND n.name LIKE '%Downey%Robert%'
    AND t.production_year > 2002
    AND k.id = mk.keyword_id
    AND t.id = mk.movie_id
    AND t.id = ci.movie_id
    AND ci.movie_id = mk.movie_id
    AND n.id = ci.person_id;;
