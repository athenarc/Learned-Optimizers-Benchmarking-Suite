import json
import socketserver
import os
from card_picker import CardPicker
from model import LeroModel
from test_script.config import LERO_DUMP_CARD_FILE
from utils import (OptState, PlanCardReplacer, get_tree_signature, print_log,
                   read_config)
import threading
import time

# --- NEW: Define temp file for communication ---
TEMP_EMBEDDING_FILE = "lero_last_embedding.json"
TEMP_FILE_LOCK = threading.Lock()

class LeroJSONHandler(socketserver.BaseRequestHandler):
    def setup(self):
        super().setup()

    def handle(self):
        str_buf = ""
        while True:
            str_buf += self.request.recv(81960).decode("UTF-8")
            if not str_buf:
                # no more data, connection is finished.
                return

            if (null_loc := str_buf.find("*LERO_END*")) != -1:
                json_msg = str_buf[:null_loc].strip()
                str_buf = str_buf[null_loc + len("*LERO_END*"):]
                if json_msg:
                    try:
                        self.handle_msg(json_msg)
                        break
                    except json.decoder.JSONDecodeError as e:
                        print(str(e))
                        print_log(
                            "Error decoding JSON:" + json_msg.replace("\"", "\'"), "./server.log", True)
                        break

    def handle_msg(self, json_msg):
        activation = {}

        def getActivation(name):
            def hook(model, input, output):
                activation[name] = output[0].detach().cpu().numpy()  # Convert to numpy
            return hook
        
        if (self.server.model is not None and
                self.server.model._net is not None):
            model_exists = True
            print("Model found, registering forward hook for DynamicPooling")
            h = self.server.model._net.module.tree_conv[8].register_forward_hook(getActivation('DynamicPooling'))
        else:
            model_exists = False
            print("No model found, skipping forward hook registration")
            h = None
        
        json_obj = json.loads(json_msg)
        msg_type = json_obj['msg_type']
        # print("Received message type:", msg_type)
        reply_msg = {}
        try:
            if msg_type == "init":
                self._init(json_obj, reply_msg)
            elif msg_type == "guided_optimization":
                self._guided_optimization(json_obj, reply_msg)
            elif msg_type == "predict":
                self._predict(json_msg, reply_msg)
            elif msg_type == "join_card":
                reply_msg['msg_type'] = "succ"
                new_card_list = self.server.opt_state_dict[json_obj['query_id']].card_picker.get_card_list()
                reply_msg['join_card'] = new_card_list
            elif msg_type == "load":
                self._load(json_obj, reply_msg)
            elif msg_type == "reset":
                self._reset(reply_msg)
            elif msg_type == "remove_state":
                self._remove_state(json_obj, reply_msg)
            else:
                print("Unknown msg type: " + msg_type)
                reply_msg['msg_type'] = "error"
        except Exception as e:
            reply_msg['msg_type'] = "error"
            reply_msg['error'] = str(e)
            print(e)

        # # After any prediction-triggering message, write the captured embedding
        # if model_exists and 'DynamicPooling' in activation:
        #     embedding = activation['DynamicPooling']
        #     embedding_data = {"embedding": embedding.tolist(), "timestamp": time.time()}
        #     with TEMP_FILE_LOCK:
        #         with open(TEMP_EMBEDDING_FILE, 'w') as f: json.dump(embedding_data, f)
        #     print(f">>> Server wrote embedding to {TEMP_EMBEDDING_FILE}")

        self.request.sendall(bytes(json.dumps(reply_msg), "utf-8"))
        self.request.close()

    def _init(self, json_obj, reply_msg):
        qid = json_obj['query_id']
        print("init query", qid)
        card_picker = CardPicker(json_obj['rows_array'], json_obj['table_array'],
                                self.server.swing_factor_lower_bound, self.server.swing_factor_upper_bound, self.server.swing_factor_step)
        # print(json_obj['table_array'], json_obj['rows_array'])
        plan_card_replacer = PlanCardReplacer(json_obj['table_array'], json_obj['rows_array'])
        opt_state = OptState(card_picker, plan_card_replacer, self.server.dump_card)
        
        self.server.opt_state_dict[qid] = opt_state
        reply_msg['msg_type'] = "succ"

    def _guided_optimization(self, json_obj, reply_msg):
        qid = json_obj['query_id']
        opt_state = self.server.opt_state_dict[qid]

        plan_card_replacer = opt_state.plan_card_replacer
        plan_card_replacer.replace(json_obj['Plan'])
        new_json_msg = json.dumps(json_obj)

        self._predict(new_json_msg, reply_msg)

        if self.server.dump_card:
            signature = str(get_tree_signature(json_obj['Plan']['Plans'][0]))
            if signature not in opt_state.visited_trees:
                card_list = opt_state.card_picker.get_card_list()
                opt_state.card_list_with_score.append(([str(card) for card in card_list], reply_msg['latency']))
                opt_state.visited_trees.add(signature)

        finish = opt_state.card_picker.next()
        reply_msg['finish'] = 1 if finish else 0

    # just do prediction
    def _predict(self, json_msg, reply_msg):
        if self.server.model is not None:
            local_features, _ = self.server.feature_generator.transform([json_msg])
            y = self.server.model.predict(local_features)
            assert y.shape == (1, 1)
            y = y[0][0]
        else:
            y = 1

        reply_msg['msg_type'] = "succ"
        reply_msg['latency'] = y

    def _load(self, json_obj, reply_msg):
        print("load new Lero model")
        model_path = json_obj['model_path']
        lero_model = LeroModel(None)
        lero_model.load(model_path)
        self.server.model = lero_model
        self.server.feature_generator = lero_model._feature_generator
        reply_msg['msg_type'] = "succ"

    def _reset(self, reply_msg):
        print("reset")
        self.server.model = None
        self.server.feature_generator = None
        reply_msg['msg_type'] = "succ"

    def _remove_state(self, json_obj, reply_msg):
        qid = json_obj['query_id']
        if self.server.dump_card:
            self._dump_card_with_score(self.server.opt_state_dict[qid].card_list_with_score)

        del self.server.opt_state_dict[qid]
        reply_msg['msg_type'] = "succ"
        # print("remove state: qid =", qid)

    def _dump_card_with_score(self, card_list_with_score):
        with open(self.server.dump_card_with_score_path, "w") as f:
            w_str = [" ".join(cards) + ";" + str(score)
                     for (cards, score) in card_list_with_score]
            w_str = "\n".join(w_str)
            f.write(w_str)

def start_server(listen_on, port, model: LeroModel):
    with socketserver.TCPServer((listen_on, port), LeroJSONHandler) as server:
        server.model = model
        server.feature_generator = model._feature_generator if model is not None else None
        server.opt_state_dict = {}

        server.best_plan = None
        server.best_score = None

        server.swing_factor_lower_bound = 0.1**2
        server.swing_factor_upper_bound = 10**2
        server.swing_factor_step = 10
        print("swing_factor_lower_bound", server.swing_factor_lower_bound)
        print("swing_factor_upper_bound", server.swing_factor_upper_bound)
        print("swing_factor_step", server.swing_factor_step)

        # dump card
        server.dump_card = True
        server.dump_card_with_score_path = LERO_DUMP_CARD_FILE

        server.serve_forever()

if __name__ == "__main__":
    config = read_config()
    port = int(config["Port"])
    listen_on = config["ListenOn"]
    print_log(f"Listening on {listen_on} port {port}", "./server.log", True)

    lero_model = None
    if "ModelPath" in config:
        lero_model = LeroModel(None)
        lero_model.load(config["ModelPath"])
        print("Load model", config["ModelPath"])

    print("start server process...")
    start_server(listen_on, port, lero_model)
