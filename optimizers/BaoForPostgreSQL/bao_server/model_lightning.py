import json
import numpy as np
import torch
import torch.optim
import joblib
import os
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.loggers import CSVLogger
from pytorch_lightning.loggers import TensorBoardLogger

from torch.utils.data import DataLoader
import net
from featurize import TreeFeaturizer

CUDA = torch.cuda.is_available()

def _nn_path(base):
    return os.path.join(base, "nn_weights")

def _x_transform_path(base):
    return os.path.join(base, "x_transform")

def _y_transform_path(base):
    return os.path.join(base, "y_transform")

def _channels_path(base):
    return os.path.join(base, "channels")

def _n_path(base):
    return os.path.join(base, "n")


def _inv_log1p(x):
    return np.exp(x) - 1

class BaoData:
    def __init__(self, data):
        assert data
        self.__data = data

    def __len__(self):
        return len(self.__data)

    def __getitem__(self, idx):
        return (self.__data[idx]["tree"],
                self.__data[idx]["target"])

def collate(x):
    trees = []
    targets = []

    for tree, target in x:
        trees.append(tree)
        targets.append(target)

    targets = torch.tensor(targets)
    return trees, targets

class BaoRegression(pl.LightningModule):
    def __init__(self, in_channels=None, verbose=False, have_cache_data=False):
        super().__init__()
        self.save_hyperparameters()
        self.verbose = verbose
        self.have_cache_data = have_cache_data
        self.fit_losses = []
        self.n = 0
        
        # Initialize transformations
        log_transformer = preprocessing.FunctionTransformer(
            np.log1p, _inv_log1p, validate=True)
        scale_transformer = preprocessing.MinMaxScaler()
        
        self.pipeline = Pipeline([
            ("log", log_transformer),
            ("scale", scale_transformer)
        ])
        
        self.tree_transform = TreeFeaturizer()
        self.in_channels = in_channels
        
        # Initialize network if channels are known
        if in_channels is not None:
            self.net = net.BaoNet(in_channels)
        else:
            self.net = None

    def forward(self, x):
        return self.net(x)
    
    def training_step(self, batch, batch_idx):
        trees, targets = batch
        predictions = self(trees)
        loss = torch.nn.functional.mse_loss(predictions, targets)
        
        # Logging
        self.log("train_loss", loss, prog_bar=True)
        self.fit_losses.append(loss.item())
        
        return loss
    
    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters())
    
    def fit(self, X, y, max_epochs=100):
        if isinstance(y, list):
            y = np.array(y)

        X = [json.loads(x) if isinstance(x, str) else x for x in X]
        self.n = len(X)
        
        # Transform targets
        y = self.pipeline.fit_transform(y.reshape(-1, 1)).astype(np.float32)
        
        # Transform features
        self.tree_transform.fit(X)
        X = self.tree_transform.transform(X)
        
        # Determine input channels if not set
        if self.in_channels is None:
            sample_tree = X[0]
            self.in_channels = sample_tree[0].shape[0]
            self.net = net.BaoNet(self.in_channels)
            if CUDA:
                self.net = self.net.cuda()
            
            if self.have_cache_data:
                assert self.in_channels == self.tree_transform.num_operators() + 3
            else:
                assert self.in_channels == self.tree_transform.num_operators() + 2

        # Create dataset
        pairs = list(zip(X, y))
        train_loader = DataLoader(
            pairs,
            batch_size=16,
            shuffle=True,
            collate_fn=collate
        )
        os.makedirs("bao_server/checkpoints", exist_ok=True)
        os.makedirs("bao_server/lightning_logs", exist_ok=True)
        
        # Set up logging and checkpointing
        logger = TensorBoardLogger("bao_server/lightning_logs", name="bao_model")
        checkpoint_callback = ModelCheckpoint(
            monitor="train_loss",
            dirpath="checkpoints",
            filename="bao-{epoch:02d}-{train_loss:.2f}",
            save_top_k=3,
            mode="min",
        )
        
        # Train the model
        trainer = pl.Trainer(
            max_epochs=max_epochs,
            logger=logger,
            callbacks=[checkpoint_callback],
            enable_progress_bar=self.verbose,
            accelerator="gpu" if CUDA else "cpu",
        )
        
        trainer.fit(self, train_loader)
        
        # Save final model
        self.save("final_model")
        
        return self
    
    def predict(self, X):
        if not isinstance(X, list):
            X = [X]
        X = [json.loads(x) if isinstance(x, str) else x for x in X]

        X = self.tree_transform.transform(X)
        
        self.eval()
        with torch.no_grad():
            pred = self(X).cpu().numpy()
        return self.pipeline.inverse_transform(pred)
    
    def save(self, path):
        os.makedirs(path, exist_ok=True)
        
        # Save PyTorch Lightning model
        ckpt_path = os.path.join(path, "model.ckpt")
        trainer = pl.Trainer()
        trainer.save_checkpoint(ckpt_path)
        
        # Save additional components
        with open(_y_transform_path(path), "wb") as f:
            joblib.dump(self.pipeline, f)
        with open(_x_transform_path(path), "wb") as f:
            joblib.dump(self.tree_transform, f)
        with open(_channels_path(path), "wb") as f:
            joblib.dump(self.in_channels, f)
        with open(_n_path(path), "wb") as f:
            joblib.dump(self.n, f)
    
    def load(self, path):
        # Load additional components first
        with open(_n_path(path), "rb") as f:
            self.n = joblib.load(f)
        with open(_channels_path(path), "rb") as f:
            self.in_channels = joblib.load(f)
        with open(_y_transform_path(path), "rb") as f:
            self.pipeline = joblib.load(f)
        with open(_x_transform_path(path), "rb") as f:
            self.tree_transform = joblib.load(f)
        
        # Load PyTorch Lightning model
        ckpt_path = os.path.join(path, "model.ckpt")
        model = BaoRegression.load_from_checkpoint(
            ckpt_path,
            in_channels=self.in_channels,
            verbose=self.verbose,
            have_cache_data=self.have_cache_data
        )
        
        # Copy state
        self.load_state_dict(model.state_dict())
        self.fit_losses = model.fit_losses
    def __init__(self, verbose=False, have_cache_data=False):
        self.__net = None
        self.__verbose = verbose
        self.__fit_losses = []  # Track losses during training

        log_transformer = preprocessing.FunctionTransformer(
            np.log1p, _inv_log1p,
            validate=True)
        scale_transformer = preprocessing.MinMaxScaler()

        self.__pipeline = Pipeline([("log", log_transformer),
                                    ("scale", scale_transformer)])
        
        self.__tree_transform = TreeFeaturizer()
        self.__have_cache_data = have_cache_data
        self.__in_channels = None
        self.__n = 0
        
    def __log(self, *args):
        if self.__verbose:
            print(*args)

    def num_items_trained_on(self):
        return self.__n
            
    def load(self, path):
        with open(_n_path(path), "rb") as f:
            self.__n = joblib.load(f)
        with open(_channels_path(path), "rb") as f:
            self.__in_channels = joblib.load(f)
            
        self.__net = net.BaoNet(self.__in_channels)
        self.__net.load_state_dict(torch.load(_nn_path(path)))
        self.__net.eval()
        
        with open(_y_transform_path(path), "rb") as f:
            self.__pipeline = joblib.load(f)
        with open(_x_transform_path(path), "rb") as f:
            self.__tree_transform = joblib.load(f)

    def save(self, path):
        # try to create a directory here
        os.makedirs(path, exist_ok=True)
        
        torch.save(self.__net.state_dict(), _nn_path(path))
        with open(_y_transform_path(path), "wb") as f:
            joblib.dump(self.__pipeline, f)
        with open(_x_transform_path(path), "wb") as f:
            joblib.dump(self.__tree_transform, f)
        with open(_channels_path(path), "wb") as f:
            joblib.dump(self.__in_channels, f)
        with open(_n_path(path), "wb") as f:
            joblib.dump(self.__n, f)

    def fit(self, X, y):
        if isinstance(y, list):
            y = np.array(y)

        X = [json.loads(x) if isinstance(x, str) else x for x in X]
        self.__n = len(X)
            
        # transform the set of trees into feature vectors using a log
        # (assuming the tail behavior exists, TODO investigate
        #  the quantile transformer from scikit)
        y = self.__pipeline.fit_transform(y.reshape(-1, 1)).astype(np.float32)
        
        self.__tree_transform.fit(X)
        X = self.__tree_transform.transform(X)

        pairs = list(zip(X, y))
        dataset = DataLoader(pairs,
                             batch_size=16,
                             shuffle=True,
                             collate_fn=collate)

        # determine the initial number of channels
        for inp, _tar in dataset:
            in_channels = inp[0][0].shape[0]
            break

        self.__log("Initial input channels:", in_channels)

        if self.__have_cache_data:
            assert in_channels == self.__tree_transform.num_operators() + 3
        else:
            assert in_channels == self.__tree_transform.num_operators() + 2

        self.__net = net.BaoNet(in_channels)
        self.__in_channels = in_channels
        if CUDA:
            self.__net = self.__net.cuda()

        optimizer = torch.optim.Adam(self.__net.parameters())
        loss_fn = torch.nn.MSELoss()
        
        losses = []
        for epoch in range(100):
            loss_accum = 0
            for x, y in dataset:
                if CUDA:
                    y = y.cuda()
                y_pred = self.__net(x)
                loss = loss_fn(y_pred, y)
                loss_accum += loss.item()
        
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            loss_accum /= len(dataset)
            losses.append(loss_accum)
            if epoch % 15 == 0:
                self.__log("Epoch", epoch, "training loss:", loss_accum)

            # stopping condition
            if len(losses) > 10 and losses[-1] < 0.1:
                last_two = np.min(losses[-2:])
                if last_two > losses[-10] or (losses[-10] - last_two < 0.0001):
                    self.__log("Stopped training from convergence condition at epoch", epoch)
                    break

            # Store the losses for metrics
            self.__fit_losses = losses
        else:
            self.__log("Stopped training after max epochs")

    def predict(self, X):
        if not isinstance(X, list):
            X = [X]
        X = [json.loads(x) if isinstance(x, str) else x for x in X]

        X = self.__tree_transform.transform(X)
        
        self.__net.eval()
        pred = self.__net(X).cpu().detach().numpy()
        return self.__pipeline.inverse_transform(pred)

    @property
    def fit_losses(self):
        return self.__fit_losses