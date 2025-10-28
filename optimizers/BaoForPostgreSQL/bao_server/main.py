import socketserver
import json
import struct
import sys
import time
import os
import storage
import model
import train
import baoctl
import math
import reg_blocker
from constants import (PG_OPTIMIZER_INDEX, DEFAULT_MODEL_PATH,
                       OLD_MODEL_PATH, TMP_MODEL_PATH)
import argparse
import threading

SERVER_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_EMBEDDING_FILE = os.path.join(SERVER_SCRIPT_DIR, "bao_last_embedding.tmp.json")
TEMP_FILE_LOCK = threading.Lock()

def add_buffer_info_to_plans(buffer_info, plans):
    for p in plans:
        p["Buffers"] = buffer_info
    return plans

class BaoModel:
    def __init__(self, log_performance=False, log_file_path="performance_log.txt"):
        self.__current_model = None
        self.log_performance = log_performance
        
        # We can still keep the analysis directory for a simple performance log,
        # but it's not strictly necessary for the embedding analysis anymore.
        self.analysis_dir = "bao_analysis"
        os.makedirs(self.analysis_dir, exist_ok=True)
        self.log_file_path = os.path.join(self.analysis_dir, log_file_path)

        if self.log_performance:
            open(self.log_file_path, "w").close()

    def log_performance_to_file(self, predicted_latency, inference_time):
        """Logs performance metrics to a file."""
        if self.log_performance:
            with open(self.log_file_path, "a") as log_file:
                log_file.write(f"Timestamp: {time.time()}, BestPredictedLatency: {predicted_latency}, InferenceTime: {inference_time}\n")

    def select_plan(self, messages):
        start = time.time()
        *arms, buffers = messages

        # if we don't have a model, default to the PG optimizer
        if self.__current_model is None:
            print("No model loaded, defaulting to PG optimizer.")
            return PG_OPTIMIZER_INDEX

        # if we do have a model, make predictions for each plan.
        arms = add_buffer_info_to_plans(buffers, arms)
        res = self.__current_model.predict(arms)
        idx = res.argmin()

        # Force index 3 which is the hash join plan
        # idx = 3
        # Force index 20 which is the nested loop plan
        # idx = 20
        # Force index 26 which is the merge join plan
        # idx = 26        
        
        best_latency = res[idx][0]
        stop = time.time()
        inference_time = stop - start

        if self.log_performance:
            self.log_performance_to_file(best_latency, inference_time)

        print(f"Selected index {idx} after {round(inference_time * 1000)}ms. "
              f"Predicted reward / PG: {res[idx][0]} / {res[0][0]}", flush=True)
        return idx

    def predict(self, messages):
        # the last message is the buffer state
        plan, buffers = messages

        # if we don't have a model, make a prediction of NaN
        if self.__current_model is None:
            return math.nan

        # if we do have a model, make predictions for each plan.
        plans = add_buffer_info_to_plans(buffers, [plan])
        res = self.__current_model.predict(plans)
        return res[0][0]
    
    def load_model(self, fp):
        # import model_lightning as model
        import model
        try:
            new_model = model.BaoRegression(have_cache_data=True)
            new_model.load(fp)

            if reg_blocker.should_replace_model(
                    self.__current_model,
                    new_model):
                self.__current_model = new_model
                print("Accepted new model.")
            else:
                print("Rejecting load of new model due to regression profile.")
                
        except Exception as e:
            print("Failed to load Bao model from", fp,
                  "Exception:", sys.exc_info()[0])
            raise e
            

class JSONTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        str_buf = ""
        while True:
            str_buf += self.request.recv(1024).decode("UTF-8")
            if not str_buf:
                # no more data, connection is finished.
                return
            
            if (null_loc := str_buf.find("\n")) != -1:
                json_msg = str_buf[:null_loc].strip()
                str_buf = str_buf[null_loc + 1:]
                if json_msg:
                    try:
                        if self.handle_json(json.loads(json_msg)):
                            break
                    except json.decoder.JSONDecodeError:
                        print("Error decoding JSON:", json_msg)
                        break


class BaoJSONHandler(JSONTCPHandler):
    def setup(self):
        super().setup()
        self.__messages = []

    def handle_json(self, data):
        activation = {}

        def getActivation(name):
            def hook(model, input, output):
                activation[name] = output[0].detach().cpu().numpy()  # Convert to numpy
            return hook

        if (self.server.bao_model._BaoModel__current_model is not None and
            hasattr(self.server.bao_model._BaoModel__current_model, '_BaoRegression__net')):
            h = self.server.bao_model._BaoModel__current_model._BaoRegression__net.tree_conv[8].register_forward_hook(
                getActivation('TreeLayerNorm'))
        else:
            h = None

        if "final" in data:
            message_type = self.__messages[0]["type"]
            self.__messages = self.__messages[1:]

            if message_type == "query":
                result = self.server.bao_model.select_plan(self.__messages)
                self.request.sendall(struct.pack("I", result))
                if 'TreeLayerNorm' in activation:
                    embedding = activation['TreeLayerNorm']
                    embedding_data = {
                        "embedding": embedding.tolist(),  # Convert to list for JSON
                        "timestamp": time.time()
                    }
                    # Use the lock to safely write to the file.
                    with TEMP_FILE_LOCK:
                        try:
                            with open(TEMP_EMBEDDING_FILE, 'w') as f:
                                json.dump(embedding_data, f)
                            # print(f"Successfully wrote embedding to {TEMP_EMBEDDING_FILE}")
                        except Exception as e:
                            print(f"ERROR: Failed to write to temp embedding file: {e}")
                self.request.close()
            elif message_type == "predict":
                result = self.server.bao_model.predict(self.__messages)
                self.request.sendall(struct.pack("d", result))
                self.request.close()
            elif message_type == "reward":
                plan, buffers, obs_reward = self.__messages
                plan = add_buffer_info_to_plans(buffers, [plan])[0]
                storage.record_reward(plan, obs_reward["reward"], obs_reward["pid"])
            elif message_type == "load model":
                path = self.__messages[0]["path"]
                print("Loading model from", path)
                self.server.bao_model.load_model(path)
            else:
                print("Unknown message type:", message_type)
            
            return True

        self.__messages.append(data)
        return False
                

def start_server(listen_on, port, log_performance=False, log_file_path="performance_log.txt"):
    model = BaoModel(log_performance=log_performance, log_file_path=log_file_path)

    if os.path.exists(DEFAULT_MODEL_PATH):
        print("Loading existing model")
        model.load_model(DEFAULT_MODEL_PATH)
    
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((listen_on, port), BaoJSONHandler) as server:
        server.bao_model = model
        server.serve_forever()


if __name__ == "__main__":
    from multiprocessing import Process
    from config import read_config
    import torch.multiprocessing as mp
    mp.set_start_method('spawn', force=True)    

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the BaoServer with an option to log performance metrics.")
    parser.add_argument('--log-performance', action='store_true', help="Enable logging of the best predicted latency and inference time.")
    parser.add_argument('--log-file-path', type=str, default="performance_log.txt", help="File path to save the performance log.")
    
    args = parser.parse_args()

    config = read_config()
    port = int(config["Port"])
    listen_on = config["ListenOn"]

    print(f"Listening on {listen_on} port {port}")
    
    server = Process(target=start_server, args=[listen_on, port, args.log_performance, args.log_file_path])
    
    print("Spawning server process...")
    server.start()