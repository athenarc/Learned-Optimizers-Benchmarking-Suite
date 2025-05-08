
import os
from torch import nn
import numpy as np
from utils.Connection import all_tables
from utils.PlanTree import OPERATOR_TYPE
import torch
import random
import itertools
from torch.distributions import Categorical
from utils.DNNRLGOO import DNNRLGOO

class DNNRLGOO_Pi(DNNRLGOO):
    def __init__(self, model, optimizer, table, query_encode, selectivity, join_table, file_name, table_alias,
                 target_model, test=False, random_num_=0):
        super().__init__(model, optimizer, table, query_encode, selectivity, join_table, file_name, table_alias,
                         target_model, test)
        self.first_state = [self.query_encode, (tuple([0] * (21 * 2 + 3)),)]
        self.choice2vector = {(0, None): 0, (1, None): 1, (0, 0): 2, (1, 0): 3, (0, 1): 4, (1, 1): 5, (0, 2): 6,
                              (1, 2): 7}
        self.vector2choice = dict([val, key] for key, val in self.choice2vector.items())
        self.table2alias = dict([val, key] for key, val in self.table_alias.items())
        self.saved_log_probs = []
        self.first_relation = []
        self.entropy = []
        self.random_num_ = random_num_

    def init_state(self):
        start = [1]
        probs = self.value_net(([self.first_state], [self.tables], start))
        m = Categorical(probs)
        self.entropy.append(float(m.entropy()))
        # action_idx = torch.argmax(probs, dim=1)
        if self.test:
            action_idx = torch.argmax(probs, dim=1)
        else:
            action_idx = m.sample()
            # print(probs)
            # print(probs[0][action_idx])
            # print('*')
        self.saved_log_probs.append(m.log_prob(action_idx))
        table_idx = action_idx // len(self.choice2vector)
        # ["Hash Join", "Merge Join", "Nested Loop"]

        if random.random() < self.random_num_:
            table_availble_ = [all_tables[table] for table in self.tables]
            table_idx = random.choice(table_availble_)

        idx, join_type = self.vector2choice[int(action_idx) % len(self.choice2vector)]
        assert join_type == None
        vector_parent = [0] * (len(all_tables) * 2 + len(self.join_type))
        vector_parent[len(self.join_type) + table_idx * 2] = 1
        vector_parent[len(self.join_type) + 1 + table_idx * 2] = idx
        self.total_vector = vector_parent.copy()
        self.collection = (tuple(vector_parent),)
        self.state_list.append((tuple(vector_parent),))
        # self.first_relation.append(list(all_tables.keys())[int(table_idx)])
        alias_name = self.table2alias[list(all_tables.keys())[int(table_idx)]]
        self.first_relation.append(alias_name)
        if idx == 1:
            self.add_hint_index(alias_name)
        self.hint_leading.append(alias_name)
        self.used_table += [alias_name]

        self.check_state()

    def choose_action(self):
        start = [0]
        operator_num = len(self.join_type)
        now_need_join_table = []
        for i in range(len(self.join_table)):
            # delete redundant join relationship
            current = [relation[0] for relation in now_need_join_table]
            if self.join_table[i][0] in self.used_table and \
                    self.join_table[i][1] not in self.used_table:
                if self.join_table[i][1] not in current:
                    now_need_join_table.append([self.join_table[i][1], i])
            if self.join_table[i][1] in self.used_table and \
                    self.join_table[i][0] not in self.used_table:
                if self.join_table[i][0] not in current:
                    now_need_join_table.append([self.join_table[i][0], i])

        # table_availbale = [[i[0] for i in now_need_join_table]]
        table_availbale = [[self.table_alias[i[0]] for i in now_need_join_table]]
        probs = self.value_net(([[self.query_encode, self.state_list[-1]]], table_availbale, start))
        m = Categorical(probs)
        self.entropy.append(float(m.entropy()))
        if self.test:
            action_idx = torch.argmax(probs, dim=1)
        else:
            action_idx = m.sample()
            # print(probs)
            # print(probs[0][action_idx])
            # print('*')
        self.saved_log_probs.append(m.log_prob(action_idx))
        table_idx = int(action_idx) // len(self.choice2vector)
        idx, join_type = self.vector2choice[int(action_idx) % len(self.choice2vector)]

        # ["Hash Join", "Merge Join", "Nested Loop"]
        num_ = random.random()
        if num_ < self.random_num_:
            table_availble_ = [all_tables[table] for table in table_availbale[0]]
            table_idx = random.choice(table_availble_)
            join_type = 0
        vector_right = [0] * (len(all_tables) * 2 + operator_num)
        vector_right[len(self.join_type) + table_idx * 2] = 1
        vector_right[len(self.join_type) + 1 + table_idx * 2] = idx
        tuple_right = (tuple(vector_right),)
        vector_parent = self.total_vector.copy()
        vector_parent[0] = vector_parent[1] = vector_parent[2] = 0
        vector_parent[operator_num + table_idx * 2] = 1
        vector_parent[operator_num + 1 + table_idx * 2] = idx
        vector_parent[join_type] = 1

        self.total_vector = vector_parent
        self.state_list.append((tuple(vector_parent), self.collection, tuple_right))
        self.collection = (tuple(vector_parent), self.collection, tuple_right)
        # alias_name = self.table2alias[list(all_tables.keys())[table_idx]]
        for table, idx in now_need_join_table:
            if self.table_alias[table] == list(all_tables.keys())[table_idx]:
                alias_name = table
        join_index = [i[0] for i in now_need_join_table].index(alias_name)
        if idx == 1:
            self.add_hint_index(alias_name)
        self.add_hint_join(alias_name, join_type)
        self.used_table.append(alias_name)
        self.join_table.pop(now_need_join_table[join_index][1])

        self.check_state()
