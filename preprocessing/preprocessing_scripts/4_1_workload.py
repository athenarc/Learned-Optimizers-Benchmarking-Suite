movie_company_queries = [
    # 1-join
    """SELECT mc.*
    FROM movie_companies mc, company_name cn
    WHERE mc.company_id = cn.id AND mc.company_id < ?""",
    
    # 2-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_id < ?""",
    
    # 3-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND mc.company_id < ?""",
    
    # 4-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND mc.company_id < ?""",
    
    # 5-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND mc.company_id < ?""",
    
    # 6-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND mc.company_id < ?""",
    
    # 7-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND mc.company_id < ?""",
    
    # 8-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND mc.company_id < ?""",
    
    # 9-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mc.company_id < ?""",
    
    # 10-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk, keyword k
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND mc.company_id < ?""",
    
    # 11-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk, keyword k, movie_info mi
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND mc.company_id < ?""",
    
    # 12-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk, keyword k, movie_info mi, info_type it
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND mi.info_type_id = it.id AND mc.company_id < ?"""
]

cast_info_scans = [
    # 1-join
    """SELECT ci.*
    FROM cast_info ci, name n
    WHERE ci.person_id = n.id AND ci.person_id < ?""",
    
    # 2-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND ci.person_id < ?""",
    
    # 3-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.person_id < ?""",
    
    # 4-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_id < ?""",
    
    # 5-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND ci.person_id < ?""",
    
    # 6-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND ci.person_id < ?""",
    
    # 7-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND ci.person_id < ?""",
    
    # 8-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND ci.person_id < ?""",
    
    # 9-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND ci.person_id < ?""",
    
    # 10-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk, keyword k
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND ci.person_id < ?""",
    
    # 11-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk, keyword k, movie_info mi
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND ci.person_id < ?""",
    
    # 12-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk, keyword k, movie_info mi, info_type it
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND mi.info_type_id = it.id AND ci.person_id < ?"""
]


queries = [
    # 1-join
    """SELECT mk.*
    FROM movie_keyword mk, keyword k
    WHERE mk.keyword_id = k.id AND mk.keyword_id < ?""",
    
    # 2-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND mk.keyword_id < ?""",
    
    # 3-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND mk.keyword_id < ?""",
    
    # 4-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mk.keyword_id < ?""",
    
    # 5-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mk.keyword_id < ?""",
    
    # 6-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND mk.keyword_id < ?""",
    
    # 7-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND mk.keyword_id < ?""",
    
    # 8-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND mk.keyword_id < ?""",
    
    # 9-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND mk.keyword_id < ?""",
    
    # 10-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt, char_name chn
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND mk.keyword_id < ?""",
    
    # 11-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt, char_name chn, movie_info mi
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mi.movie_id AND mk.keyword_id < ?""",
    
    # 12-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt, char_name chn, movie_info mi, info_type it
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mi.movie_id AND mi.info_type_id = it.id AND mk.keyword_id < ?"""
]