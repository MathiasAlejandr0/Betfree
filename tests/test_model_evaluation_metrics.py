import numpy as np

from src.predictor.model_evaluation_metrics import multiclass_log_loss_stable


def test_multiclass_log_loss_perfect() -> None:
    y = np.array([0, 1, 2], dtype=np.int64)
    p = np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], dtype=np.float64)
    ll = multiclass_log_loss_stable(y, p)
    assert ll < 1e-6
