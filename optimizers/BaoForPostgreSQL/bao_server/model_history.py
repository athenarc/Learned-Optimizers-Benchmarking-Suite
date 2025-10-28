# model_history.py
import json
import os
from datetime import datetime

class ModelHistory:
    def __init__(self, history_dir="model_history"):
        self.history_dir = history_dir
        os.makedirs(history_dir, exist_ok=True)
        
    def record_training(self, model_info):
        """Record a training session"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        history_path = os.path.join(self.history_dir, f"training_{timestamp}.json")
        
        with open(history_path, 'w') as f:
            json.dump(model_info, f, indent=2)
            
    def get_training_history(self):
        """Get all recorded training sessions"""
        history = []
        for fname in sorted(os.listdir(self.history_dir)):
            if fname.startswith('training_') and fname.endswith('.json'):
                with open(os.path.join(self.history_dir, fname)) as f:
                    history.append(json.load(f))
        return history