SELECT mc.company_id
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND mc.company_id < 73232