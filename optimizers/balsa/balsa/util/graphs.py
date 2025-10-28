# Copyright 2022 The Balsa Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# table -> list of neighbors.  Ref: journal version of Leis et al.
JOIN_ORDER_BENCHMARK_JOIN_GRAPH = {
    'aka_title': ['title'],
    'char_name': ['cast_info'],
    'role_type': ['cast_info'],
    'comp_cast_type': ['complete_cast'],
    'movie_link': ['title', 'link_type'] + [
        'complete_cast', 'aka_title', 'movie_link', 'cast_info',
        'movie_companies', 'movie_keyword', 'movie_info_idx', 'movie_info',
        'kind_type'
    ],  # movie_link.id linked to title.id which are both primary keys
    'link_type': ['movie_link'],
    'cast_info': ['char_name', 'role_type', 'title', 'aka_name'],
    'complete_cast': ['comp_cast_type', 'title'],
    'title': [
        'complete_cast', 'aka_title', 'movie_link', 'cast_info',
        'movie_companies', 'movie_keyword', 'movie_info_idx', 'movie_info',
        'kind_type'
    ],
    'aka_name': ['cast_info', 'name'],
    'movie_companies': ['title', 'company_name', 'company_type'],
    'kind_type': ['title'],
    'name': ['aka_name', 'person_info'] +
    ['cast_info'],  # name.id linked to aka_name.id which are both primary keys
    'company_type': ['movie_companies'],
    'movie_keyword': ['title', 'keyword'],
    'movie_info': ['title', 'info_type'],
    'person_info': ['name', 'info_type'],
    'info_type': ['movie_info', 'person_info', 'movie_info_idx'],
    'company_name': ['movie_companies'],
    'keyword': ['movie_keyword'],
    'movie_info_idx': ['title', 'info_type'],
}

STACK_JOIN_GRAPH = {
    'account': ['so_user'],
    'answer': ['site', 'so_user', 'question'],
    'badge': ['site', 'so_user'],
    'comment': ['site'],
    'post_link': ['site', 'question'],
    'question': ['answer', 'post_link', 'tag_question', 'site', 'so_user'],
    'site': ['site', 'answer', 'badge', 'comment', 'post_link', 'question', 'so_user', 'tag', 'tag_question'],
    'so_user': ['account', 'answer', 'badge', 'question'],
    'tag': ['site', 'tag_question'],
    'tag_question': ['site', 'tag', 'question'],
}

TPC_H_JOIN_GRAPH = {
    'customer': ['orders', 'nation'],
    'lineitem': ['partsupp', 'orders'],
    'nation': ['customer', 'region', 'supplier'],
    'orders': ['customer', 'lineitem'],
    'part': ['partsupp'],
    'partsupp': ['part', 'supplier', 'lineitem'],
    'region': ['nation'],
    'supplier': ['nation', 'partsupp'],
}

TPC_DS_JOIN_GRAPH = {
    'call_center': ['date_dim', 'catalog_returns', 'catalog_sales'],
    'catalog_page': ['date_dim', 'promotion', 'catalog_returns', 'catalog_sales'],
    'catalog_returns': [
        'call_center', 'catalog_page', 'item', 'reason', 'customer_address',
        'customer_demographics', 'customer', 'household_demographics', 'date_dim',
        'time_dim', 'ship_mode', 'warehouse'
    ],
    'catalog_sales': [
        'customer_address', 'customer_demographics', 'customer', 'household_demographics',
        'call_center', 'catalog_page', 'item', 'promotion', 'date_dim', 'time_dim',
        'ship_mode', 'warehouse'
    ],
    'customer': [
        'customer_address', 'customer_demographics', 'household_demographics', 'date_dim',
        'store_returns', 'store_sales', 'catalog_returns', 'catalog_sales',
        'web_returns', 'web_sales'
    ],
    'customer_address': [
        'customer', 'catalog_returns', 'catalog_sales', 'store_returns', 'store_sales',
        'web_returns', 'web_sales'
    ],
    'customer_demographics': [
        'customer', 'catalog_returns', 'catalog_sales', 'store_returns', 'store_sales',
        'web_returns', 'web_sales'
    ],
    'date_dim': [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer',
        'inventory', 'promotion', 'store', 'store_returns', 'store_sales', 'web_page',
        'web_returns', 'web_sales', 'web_site'
    ],
    'household_demographics': [
        'income_band', 'customer', 'catalog_returns', 'catalog_sales',
        'store_returns', 'store_sales', 'web_returns', 'web_sales'
    ],
    'income_band': ['household_demographics'],
    'inventory': ['date_dim', 'item', 'warehouse'],
    'item': [
        'catalog_returns', 'catalog_sales', 'inventory', 'promotion', 'store_returns',
        'store_sales', 'web_returns', 'web_sales'
    ],
    'promotion': [
        'date_dim', 'item', 'catalog_page', 'catalog_sales', 'store_sales', 'web_sales'
    ],
    'reason': ['catalog_returns', 'store_returns', 'web_returns'],
    'ship_mode': ['catalog_returns', 'catalog_sales', 'web_returns', 'web_sales'],
    'store': ['store_returns', 'store_sales'],
    'store_returns': [
        'customer_address', 'customer_demographics', 'customer', 'household_demographics',
        'item', 'reason', 'date_dim', 'time_dim', 'store'
    ],
    'store_sales': [
        'customer_address', 'customer_demographics', 'customer', 'household_demographics',
        'item', 'promotion', 'date_dim', 'time_dim', 'store'
    ],
    'time_dim': [
        'catalog_returns', 'catalog_sales', 'store_returns', 'store_sales',
        'web_returns', 'web_sales'
    ],
    'warehouse': [
        'catalog_returns', 'catalog_sales', 'inventory', 'web_returns', 'web_sales'
    ],
    'web_page': [
        'date_dim', 'web_returns', 'web_sales'
    ],
    'web_returns': [
        'item', 'reason', 'customer_address', 'customer_demographics', 'customer',
        'household_demographics', 'date_dim', 'time_dim', 'web_page', 'warehouse',
        'ship_mode'
    ],
    'web_sales': [
        'customer_address', 'customer_demographics', 'customer', 'household_demographics',
        'item', 'promotion', 'date_dim', 'time_dim', 'warehouse', 'ship_mode',
        'web_page', 'web_site'
    ],
    'web_site': ['date_dim', 'web_sales']
}

SSB_JOIN_GRAPH = {
    'lineorder': ['customer', 'supplier', 'part', 'date'],
    'lineitem': ['orders', 'part', 'supplier', 'date'],
    'orders': ['lineitem', 'customer', 'date'],
    'customer': ['lineorder', 'orders'],
    'supplier': ['lineorder', 'lineitem'],
    'part': ['lineorder', 'lineitem'],
    'date': ['lineorder', 'lineitem', 'orders'],
}