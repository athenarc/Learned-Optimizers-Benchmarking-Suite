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

def add_buffer_info_to_plans(buffer_info, plans):
    for p in plans:
        p["Buffers"] = buffer_info
    return plans

class BaoModel:
    def __init__(self, log_performance=False, log_file_path="performance_log.txt"):
        self.__current_model = None
        self.log_performance = log_performance  # New parameter to control logging
        self.log_file_path = log_file_path  # Where to save the logs
        if self.log_performance:
            open(log_file_path, "w").close()
        self.embeddings_dir = "plan_embeddings"  # Directory to save plan embeddings
        os.makedirs(self.embeddings_dir, exist_ok=True)
        self.query_count = 0  # Counter to track the number of queries

    def log_performance_to_file(self, predicted_latency, inference_time):
        """Logs the best predicted latency and inference time to a file with the query count if enabled."""
        if self.log_performance:
            with open(self.log_file_path, "a") as log_file:
                log_file.write(f"{time.time()} - Query #{self.query_count} - Best Predicted Latency: {predicted_latency} - Inference Time: {inference_time}\n")

    def store_best_plan(self, query_num, plan_data):
        """Save individual plan data to a JSON file"""
        filename = f"query_{query_num}_embedding.json"
        filepath = os.path.join(self.embeddings_dir, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(plan_data, f, indent=2)
        except Exception as e:
            print(f"Failed to save plan embedding for query {query_num}: {str(e)}")

    def select_plan(self, messages):
        start = time.time()
        # the last message is the buffer state
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
        best_plan = arms[idx]
        if self.log_performance:
            self.log_performance_to_file(best_latency, inference_time)
            if hasattr(self.__current_model, 'get_plan_embedding'):
                try:
                    embedding = self.__current_model.get_plan_embedding(best_plan)
                    self.store_best_plan(self.query_count, {
                        'embedding': embedding,
                        'plan': best_plan,
                        'predicted_latency': best_latency,
                        'inference_time': inference_time,
                        'timestamp': time.time()
                    })
                except Exception as e:
                    print(f"Failed to extract embedding: {str(e)}")
        self.query_count += 1
        print("Selected index", idx,
              "after", f"{round((stop - start) * 1000)}ms",
              "Predicted reward / PG:", res[idx][0],
              "/", res[0][0])
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
        self.__messages = []
    
    def handle_json(self, data):
        if "final" in data:
            message_type = self.__messages[0]["type"]

            self.__messages = self.__messages[1:]
            if message_type == "query":
                result = self.server.bao_model.select_plan(self.__messages)
                print("Sending index", result)
                self.request.sendall(struct.pack("I", result))
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