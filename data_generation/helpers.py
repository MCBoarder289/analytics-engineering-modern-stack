import numpy as np


def generate_nps(rng: np.random.Generator , transfer: bool, hold_time: int, previous_issue_flag: bool) -> int:
    # Base probabilities
    p_promoter = 0.6
    p_passive  = 0.2
    p_detractor = 0.2

    # You can adjust these probs by call quality:
    if transfer or hold_time > 120 or previous_issue_flag:
        p_promoter -= 0.2
        p_detractor += 0.2

    category = rng.choice(
        ["promoter", "passive", "detractor"],
        p=[p_promoter, p_passive, p_detractor]
    )

    if category == "promoter":
        return int(rng.choice([9, 10], p=[0.5, 0.5]))
    elif category == "passive":
        return int(rng.choice([7, 8], p=[0.5, 0.5]))
    else:  # detractor
        return int(rng.integers(1, 7))
