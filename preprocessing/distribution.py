from functools import cached_property
from typing import Any, Dict, List, Optional, Sequence
import pandas as pd
import numpy as np
import random
import os
import numpy as np
from dist import categorical_dist_on_predefined_bins, categorical_dist, numerical_dist_on_predefined_bins

def seed_everything(seed: int):
    random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)
    np.random.seed(seed)
    
seed_everything(42)    

sample_rng = np.random.default_rng(np.random.MT19937(seed=np.random.randint(0, 2**32)))

class MetadataDistributionHelpers:
    def __init__(self, series: pd.Series, slots: Optional[List[str]] = None):
        self._series = series
        self._slots = slots

    @cached_property
    def dist(self) -> pd.Series:
        if self._slots:
            series = self._series.astype(str)
            d = categorical_dist_on_predefined_bins(series, bins=self._slots)
        else:
            d = categorical_dist(self._series)

        return d
    

class MetadataDistribution:
    def __init__(
            self,
            type: str,
            data: Sequence[Any],
            slots: Optional[List[str]] = None,
    ):
        self._type = type
        self._data = data
        tuple_data = [tuple(combo) for combo in data]  # Sort for consistency
        print(f"MetadataDistribution: {self._type} with {len(data)} items")
        # self._series = pd.Series(self._data)
        self._series = pd.Series(tuple_data)
        self._helpers = MetadataDistributionHelpers(series=self._series, slots=slots)

    def get(self) -> pd.Series:
        return self._helpers.dist

    def __len__(self):
        return len(self._data)

    @property
    def bin_values(self):
        return self._helpers.dist.index.values

    def __repr__(self):
        return f"""Dist of Metadata {self._type}, num_combos={len(self._data)}:
{self.get()}
"""

import itertools
import time
from typing import Generator, List, Sequence
import typing
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

def split_into_batches(lst, batch_size):
    """
    Splits a list into batches of a given size.

    Parameters:
        lst (list): The list to be split.
        batch_size (int): The size of each batch.

    Returns:
        list: A list of batches.
    """
    return [lst[i : i + batch_size] for i in range(0, len(lst), batch_size)]


def para_sample(values: Generator, random_state: int):
    rng = np.random.default_rng(np.random.MT19937(seed=random_state))

    result = []

    for v in values:
        if not issubclass(v.__class__, pd.Interval):
            result.append(v)
            continue

        if isinstance(v.left, float):
            result.append(rng.uniform(v.left, v.right))
        elif isinstance(v.left, int):
            result.append(rng.integers(v.left, v.right))
        elif isinstance(v.left, np.integer):
            result.append(rng.integers(int(v.left), int(v.right)))
        else:
            raise ValueError(f"Unsupported value type: {type(v.left)}")

    return result


def sample_from_distribution(distribution: List[float], values: list, num_samples: int):
    """
    Sample data from a given distribution and pair it with corresponding values.

    Args:
        distribution (array-like): The probability distribution to sample from.
        values (array-like): The corresponding values for each probability in the distribution.
        num_samples (int): The number of samples to generate.

    Returns:
        tuple: (sampled_values, sampled_indices)
            sampled_values: The sampled corresponding values.
            sampled_indices: The indices of the sampled items (for reference).
    """
    if len(distribution) != len(values):
        raise ValueError("The length of distribution and values must be the same.")

    # Ensure the distribution sums to 1
    distribution = np.array(distribution) / np.sum(distribution)

    # Sample indices based on the distribution
    start_time = time.time()
    
    sampled_indices = sample_rng.choice(
        len(distribution), size=num_samples, p=distribution
    )
    print(f"Sampling time: {time.time() - start_time}")

    # Get the corresponding values for the sampled indices
    start_time = time.time()
    
    if num_samples < 32768:
        """Sequential Sampling"""
        result = para_sample((values[i] for i in sampled_indices), 1)
    
    else:
        """Parallel Sampling"""
        result = list(
            itertools.chain.from_iterable(
                typing.cast(
                    List,
                    Parallel(n_jobs=-1, batch_size=8192, verbose=10)(
                        delayed(para_sample)(b, i+1)
                        for i, b in enumerate(split_into_batches(sampled_indices, 8192))
                    ),
                )
            )
        )

    print(f"Reassigning time: {time.time() - start_time}")

    return result, sampled_indices


if __name__ == "__main__":
    distribution = [0.1, 0.2, 0.3, 0.4]
    values = ["a", "b", "c", "d"]
    num_samples = 1000
    sampled_values, sampled_indices = sample_from_distribution(
        distribution, values, num_samples
    )

    # print(sampled_values)
    print(sampled_indices)
    count = np.unique(sampled_indices, return_counts=True)[1] / num_samples
    print(count)

