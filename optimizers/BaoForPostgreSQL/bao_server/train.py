import storage
# import model_lightning as model
import model
import os
import shutil
import reg_blocker
import json
from datetime import datetime
import time
import numpy as np

class BaoTrainingException(Exception):
    pass

def save_training_metadata(model_path, metadata):
    """Save training metadata with proper type conversion for JSON serialization"""
    meta_path = f"{model_path}.metadata.json"
    
    def convert_numpy_types(obj):
        if isinstance(obj, (np.generic, np.ndarray)):
            return obj.item() if obj.size == 1 else obj.tolist()
        elif isinstance(obj, dict):
            return {k: convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert_numpy_types(x) for x in obj]
        return obj
    
    try:
        with open(meta_path, 'w') as f:
            json.dump(convert_numpy_types(metadata), f, indent=2, default=str)  # Handle non-serializable objects
    except Exception as e:
        print(f"Warning: Could not save metadata to {meta_path}: {str(e)}")

def get_training_metadata(model_path):
    """Safely load training metadata, returns empty dict if missing/invalid"""
    meta_path = f"{model_path}.metadata.json"
    if not os.path.exists(meta_path):
        return {}
    
    try:
        with open(meta_path) as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Warning: Invalid JSON in {meta_path}")
        return {}
    except Exception as e:
        print(f"Warning: Could not load metadata from {meta_path}: {str(e)}")
        return {}

def archive_model(model_path, archive_dir="model_archive"):
    """Archive the model with timestamp"""
    os.makedirs(archive_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_name = os.path.basename(model_path)
    archive_path = os.path.join(archive_dir, f"{timestamp}_{model_name}")
    
    if os.path.isdir(model_path):
        shutil.copytree(model_path, archive_path)
    else:
        shutil.copy(model_path, archive_path)
    
    # Also copy metadata if it exists
    meta_path = f"{model_path}.metadata.json"
    if os.path.exists(meta_path):
        shutil.copy(meta_path, f"{archive_path}.metadata.json")
    
    return archive_path

def train_and_swap(fn, old, tmp, verbose=False):
    old_metadata = get_training_metadata(fn)
    if os.path.exists(fn):
        old_model = model.BaoRegression(have_cache_data=True)
        old_model.load(fn)
    else:
        old_model = None

    # Train new model
    try:
        new_model, training_metrics = train_and_save_model(tmp, verbose=verbose)
    except Exception as e:
        print(f"Error during training: {str(e)}")
        raise
    
    # Create training metadata
    metadata = {
        'training_date': datetime.now().isoformat(),
        'training_samples': len(new_model.training_data) if hasattr(new_model, 'training_data') else None,
        'metrics': training_metrics,
        'previous_version': old_metadata.get('training_date', 'unknown')
    }
    
    max_retries = 5
    current_retry = 1
    while not reg_blocker.should_replace_model(old_model, new_model):
        if current_retry >= max_retries == 0:
            print("Could not train model with better regression profile.")
            return
        
        print("New model rejected when compared with old model. "
              + "Trying to retrain with emphasis on regressions.")
        print("Retry #", current_retry)
        try:
            new_model, training_metrics = train_and_save_model(tmp, verbose=verbose,
                                            emphasize_experiments=current_retry)
            metadata['retry_attempts'] = current_retry
            current_retry += 1
        except Exception as e:
            print(f"Error during retry training: {str(e)}")
            raise

    # Archive old model before replacing
    if os.path.exists(fn):
        try:
            archive_model(fn)
            shutil.rmtree(old, ignore_errors=True)
            os.rename(fn, old)
            old_meta = f"{fn}.metadata.json"
            if os.path.exists(old_meta):
                os.rename(old_meta, f"{old}.metadata.json")
        except Exception as e:
            print(f"Warning: Could not archive old model: {str(e)}")
    
    # Save new model
    try:
        os.rename(tmp, fn)
        save_training_metadata(fn, metadata)
        if verbose:
            print(f"Model successfully updated at {fn}")
    except Exception as e:
        print(f"Error saving new model: {str(e)}")
        raise

def train_and_save_model(fn, verbose=True, emphasize_experiments=0):
    all_experience = storage.experience()

    start_data_time = time.time()
    for _ in range(emphasize_experiments):
        all_experience.extend(storage.experiment_experience())
    data_collection_time = time.time() - start_data_time
    
    x = [i[0] for i in all_experience]
    y = [i[1] for i in all_experience]        
    
    if not all_experience:
        raise BaoTrainingException("Cannot train a Bao model with no experience")
    
    if len(all_experience) < 20:
        print("Warning: trying to train a Bao model with fewer than 20 datapoints.")

    reg = model.BaoRegression(have_cache_data=True, verbose=verbose)

    start_train_time = time.time()
    reg.fit(x, y)
    training_time = time.time() - start_train_time

    # Get predictions for metrics calculation
    predictions = reg.predict(x)
    y_true = np.array(y)
    y_pred = np.array(predictions).flatten()

    # Convert metrics to JSON-serializable types
    metrics = {
        'training': {
            'samples': int(len(y_true)),  # Convert to Python int
            'epochs': int(len(reg.fit_losses)),
            'final_loss': reg.fit_losses[-1],
            'min_loss': min(reg.fit_losses),            
            'time_seconds': float(training_time),
            'data_collection_time': float(data_collection_time),
        },
        'performance': {
            'mae': float(np.mean(np.abs(y_true - y_pred))),
            'mse': float(np.mean((y_true - y_pred)**2)),
            'r2': float(1 - (np.sum((y_true - y_pred)**2) / np.sum((y_true - np.mean(y_true))**2))),
            'q_errors': {k: float(v) for k, v in calculate_q_error_stats(y_true, y_pred).items()},
            'relative_errors': {k: float(v) for k, v in calculate_relative_error_stats(y_true, y_pred).items()}
        },
        'data_stats': {
            'target_mean': float(np.mean(y_true)),
            'target_std': float(np.std(y_true)),
            'target_min': float(np.min(y_true)),
            'target_max': float(np.max(y_true)),
            'prediction_mean': float(np.mean(y_pred)),
            'prediction_std': float(np.std(y_pred))
        }
    }
    
    # Store training data with the model for reference
    reg.training_data = all_experience
    reg.save(fn)
    
    return reg, metrics

def calculate_q_error_stats(y_true, y_pred):
    """Calculate Q-error statistics (max(pred/true, true/pred))"""
    with np.errstate(divide='ignore', invalid='ignore'):
        ratios = np.maximum(y_pred / y_true, y_true / y_pred)
        valid_ratios = ratios[np.isfinite(ratios)]
    
    if len(valid_ratios) == 0:
        return {'min': 0, 'median': 0, 'mean': 0, 'max': 0, '90th_percentile': 0}
    
    return {
        'min': np.min(valid_ratios),
        'median': np.median(valid_ratios),
        'mean': np.mean(valid_ratios),
        'max': np.max(valid_ratios),
        '90th_percentile': np.percentile(valid_ratios, 90)
    }

def calculate_relative_error_stats(y_true, y_pred):
    """Calculate relative error statistics ((true - pred)/true)"""
    with np.errstate(divide='ignore', invalid='ignore'):
        rel_errors = (y_true - y_pred) / y_true
        valid_errors = rel_errors[np.isfinite(rel_errors)]
    
    if len(valid_errors) == 0:
        return {'min': 0, 'median': 0, 'mean': 0, 'max': 0, '90th_percentile': 0}
    
    abs_errors = np.abs(valid_errors)
    return {
        'min': np.min(abs_errors),
        'median': np.median(abs_errors),
        'mean': np.mean(abs_errors),
        'max': np.max(abs_errors),
        '90th_percentile': np.percentile(abs_errors, 90)
    }
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: train.py MODEL_FILE")
        exit(-1)
    train_and_save_model(sys.argv[1])

    print("Model saved, attempting load...")
    reg = model.BaoRegression(have_cache_data=True)
    reg.load(sys.argv[1])

