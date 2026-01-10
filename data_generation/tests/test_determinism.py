import datetime
import os
from pathlib import Path

import pandas as pd
import pytest

from data_generation import call_center_simulation

# NOTE: In Pycharm, mark the tests directory as "Test Sources Root" to make these run from the gutter


def test_generate_customers_determinism():
    config = call_center_simulation.SimulationConfig(customers_count=10)
    df1 = config.generate_customers()
    df2 = config.generate_customers()
    pd.testing.assert_frame_equal(df1, df2)

    different_seed_df = config.generate_customers(faker_seed=111)
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(df1, different_seed_df)


def test_generate_agents_determinism():
    config = call_center_simulation.SimulationConfig(agents_count=10)
    df1 = config.generate_agents()
    df2 = config.generate_agents()
    pd.testing.assert_frame_equal(df1, df2)

    different_seed_df = config.generate_agents(faker_seed=111)
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(df1, different_seed_df)


def test_generate_managers_determinism():
    config = call_center_simulation.SimulationConfig(managers_count=10)
    df1 = config.generate_managers()
    df2 = config.generate_managers()
    pd.testing.assert_frame_equal(df1, df2)

    different_seed_df = config.generate_managers(faker_seed=111)
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(df1, different_seed_df)


def test_agent_assignments_determinism():
    config = call_center_simulation.SimulationConfig(
        agents_count=10,
        managers_count=10,
    )
    agents = config.generate_agents()
    managers = config.generate_managers()

    agent_assignments1 = call_center_simulation.distribute_agents_to_managers(
        agents=agents,
        managers=managers,
        start_date=config.global_start_date,
        end_date=config.global_end_date,
    )

    agent_assignments2 = call_center_simulation.distribute_agents_to_managers(
        agents=agents,
        managers=managers,
        start_date=config.global_start_date,
        end_date=config.global_end_date,
    )

    pd.testing.assert_frame_equal(agent_assignments1, agent_assignments2)


def test_bitwise_determinism_csv_parquet(tmp_path):
    # First run
    output_dir1 = tmp_path / "run1"
    seed_dir1 = tmp_path / "seed1"
    os.makedirs(output_dir1)
    os.makedirs(seed_dir1)
    call_center_simulation.simulate_call_center(
        simulation_config=call_center_simulation.SimulationConfig(
            global_start_date=datetime.date(2025, 1, 1),
            global_end_date=datetime.date(2025, 1, 3),
            customers_count=10,
            agents_count=5,
            managers_count=2,
            rng_seed=123,
        ),
        parquet_output_dir=str(output_dir1),
        seed_output_dir=str(seed_dir1),
    )

    # Second run
    output_dir2 = tmp_path / "run2"
    seed_dir2 = tmp_path / "seed2"
    os.makedirs(output_dir2)
    os.makedirs(seed_dir2)
    call_center_simulation.simulate_call_center(
        simulation_config=call_center_simulation.SimulationConfig(
            global_start_date=datetime.date(2025, 1, 1),
            global_end_date=datetime.date(2025, 1, 3),
            customers_count=10,
            agents_count=5,
            managers_count=2,
            rng_seed=123,
        ),
        parquet_output_dir=str(output_dir2),
        seed_output_dir=str(seed_dir2),
    )

    # Compare all files bitwise
    for dir1, dir2 in [(output_dir1, output_dir2), (seed_dir1, seed_dir2)]:
        for file1 in Path(dir1).rglob("*"):
            if file1.is_file():
                rel = file1.relative_to(dir1)
                file2 = dir2 / rel
                assert file2.exists()
                assert file1.read_bytes() == file2.read_bytes()
