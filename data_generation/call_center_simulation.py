import calendar
import datetime
import os
import random
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from data_generation.constants import (
    GLOBAL_END_DATE,
    GLOBAL_START_DATE,
    PROGRAMS,
    SENTINEL_END_DATE,
    WEEKDAY_MULTIPLIERS,
)
from data_generation.helpers import generate_nps


@dataclass(frozen=True)
class SimulationConfig:
    global_start_date: datetime.date = GLOBAL_START_DATE
    global_end_date: datetime.date = GLOBAL_END_DATE
    programs: dict = field(default_factory=lambda: PROGRAMS.copy())
    agents_count: int = 50
    managers_count: int = 5
    customers_count: int | None = None  # see default setting below
    # Simulation parameters
    min_call_length: int = 20
    calls_per_agent_per_day: int = 60
    callback_rate: float = 0.1
    survey_rate: float = 0.3
    previous_issue_rate: float = 0.4
    transfer_rate: float = 0.1
    workday_start: int = 8
    workday_end: int = 17
    mean_seconds_between_calls: int = 600
    # Seasonality + weekday scaling
    seasonality_amplitude: float = 0.3  # +/- 30%
    weekday_multipliers: dict = field(default_factory=lambda: WEEKDAY_MULTIPLIERS.copy())
    # Reproducibility
    rng_seed: int = 289
    output_dir: str = "../data"
    write_csv: bool = True
    write_parquet: bool = True
    tables: list[str] = ("calls", "crm", "surveys", "agents", "managers", "agent_assignments")

    def __post_init__(self):
        if self.customers_count is None:
            object.__setattr__(
                self,
                "customers_count",
                self.agents_count * self.calls_per_agent_per_day * 20,
            )

    def with_overrides(self, **kwargs) -> "SimulationConfig":
        """Return a new SimulationConfig with some fields overridden."""
        return replace(self, **kwargs)

    def generate_customers(self, faker_seed: int = 289, random_seed: int = 315) -> pd.DataFrame:
        fake = Faker()
        Faker.seed(faker_seed)
        random.seed(random_seed)

        program_names = list(self.programs.keys())

        customer_records = []

        for i in range(self.customers_count):
            state = fake.state_abbr(include_territories=False, include_freely_associated_states=False)
            program_selection = random.choice(program_names)
            customer_record = {
                "customer_id": i + 1,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "birth_date": fake.date_between(
                    start_date=self.global_start_date - datetime.timedelta(days=77 * 365),
                    end_date=self.global_start_date - datetime.timedelta(days=21 * 365),
                ),
                "state": state,
                "zip_code": fake.zipcode_in_state(state_abbr=state),
                "program": program_selection,
            }

            customer_records.append(customer_record)

        customer_df = pd.DataFrame.from_records(customer_records)
        customer_df["zip_code"] = customer_df["zip_code"].astype(str)

        return customer_df

    def generate_agents(self, faker_seed: int = 867) -> pd.DataFrame:
        fake = Faker()
        Faker.seed(faker_seed)

        agent_records = []

        for i in range(self.agents_count):
            agent_record = {
                "agent_id": i + 1,
                "agent_name": fake.name(),
            }
            agent_records.append(agent_record)

        return pd.DataFrame.from_records(agent_records)

    def generate_managers(self, faker_seed: int = 222) -> pd.DataFrame:
        fake = Faker()
        Faker.seed(faker_seed)

        manager_records = []

        for i in range(self.managers_count):
            manager_record = {
                "manager_id": i + 1,
                "manager_name": fake.name(),
            }
            manager_records.append(manager_record)

        return pd.DataFrame.from_records(manager_records)


DEFAULT_CONFIG = SimulationConfig()


# Helper function to simulate call reasons
def get_call_reasons_plus_duration(
    simulation_config: SimulationConfig,
    program_key: str,
    rng: np.random.Generator,
    reason: str | None = None,
    subreason: str | None = None,
) -> tuple[str, str, int]:
    if reason and subreason:
        reason_record = [
            reason_record
            for reason_record in simulation_config.programs[program_key]["reasons"]
            if reason_record["name"] == reason
        ][0]
        subreason_record = [
            subreason_record
            for subreason_record in reason_record["sub_reasons"]
            if subreason_record["name"] == subreason
        ][0]

        duration = max(
            simulation_config.min_call_length,
            int(rng.normal(subreason_record["duration_mean"], subreason_record["duration_std"])),
        )

        return reason, subreason, duration
    else:
        reasons = simulation_config.programs[program_key]["reasons"]
        reason_probs = [r["prob"] for r in reasons]

        reason = rng.choice(reasons, p=reason_probs)
        sub_reasons = reason["sub_reasons"]
        sub_probs = [s["prob"] for s in sub_reasons]

        sub_reason = rng.choice(sub_reasons, p=sub_probs)

        # duration draw ~ Normal(mean, std)
        duration = max(
            simulation_config.min_call_length, int(rng.normal(sub_reason["duration_mean"], sub_reason["duration_std"]))
        )

        return str(reason["name"]), str(sub_reason["name"]), duration


def distribute_agents_to_managers(
    agents: pd.DataFrame,
    managers: pd.DataFrame,
    start_date: datetime.date,
    end_date: datetime.date,
    avg_reassignments: int = 2,
) -> pd.DataFrame:
    """Distribute agents to managers with effective date ranges and reassignments."""

    assignments = []
    rng = np.random.default_rng(315)

    date_range = pd.date_range(start_date, end_date).to_pydatetime().tolist()

    # --- Step 1: Evenly assign agents initially ---
    shuffled_agents = agents.sample(frac=1, random_state=42).reset_index(drop=True)
    manager_ids = managers["manager_id"].tolist()

    for i, agent in shuffled_agents.iterrows():
        manager_id = manager_ids[i % len(manager_ids)]  # round-robin
        current_start = pd.to_datetime(start_date)

        # --- Step 2: Generate reassignment dates ---
        n_changes = rng.integers(avg_reassignments + 1)
        change_dates = sorted(rng.choice(date_range, size=n_changes, replace=False)) if n_changes > 0 else []

        # --- Step 3: Add assignment segments ---
        for change_date in change_dates:
            assignments.append({
                "agent_id": agent["agent_id"],
                "manager_id": manager_id,
                "effective_start": current_start,
                "effective_end": change_date - datetime.timedelta(days=1),
            })

            # switch manager, exclude current
            other_mgrs = managers["manager_id"].loc[managers["manager_id"] != manager_id].to_numpy()
            manager_id = rng.choice(other_mgrs)

            current_start = change_date

        # final segment until end_date
        assignments.append({
            "agent_id": agent["agent_id"],
            "manager_id": manager_id,
            "effective_start": current_start,
            "effective_end": pd.to_datetime(SENTINEL_END_DATE),
        })

    assignments_df = pd.DataFrame(assignments)

    return assignments_df


def simulate_hold_time(rng: np.random.Generator) -> int:
    return int(np.clip(rng.normal(45, 20), 0, 300))


def write_daily_parquet(records: list[dict], output_dir: str, table: str, date: datetime.date):
    """Write records to parquet, partitioned by day."""
    if not records:
        return
    df = pd.DataFrame.from_records(records)
    day_str = date.strftime("%Y-%m-%d")
    day_dir = Path(output_dir) / table / f"day={day_str}"
    os.makedirs(day_dir, exist_ok=True)
    # Find the next part number
    existing_parts = list(day_dir.glob(f"part-*-{table}.parquet"))
    part_nums = [
        int(f.name.split("-")[1])
        for f in existing_parts
        if f.name.startswith("part-") and f.name.endswith(f"-{table}.parquet")
    ]
    next_part = max(part_nums, default=-1) + 1
    filename = day_dir / f"part-{next_part:04d}-{table}.parquet"
    df.to_parquet(filename, index=False)


def simulate_call_center(
    simulation_config: SimulationConfig = DEFAULT_CONFIG,
    parquet_output_dir: str = "../data",
    seed_output_dir: str = "../call_center/seeds",
) -> None:
    rng = np.random.default_rng(simulation_config.rng_seed)

    # Write out customers, managers, and assignments csv files for dbt seed
    agents = simulation_config.generate_agents()
    managers = simulation_config.generate_managers()
    customers = simulation_config.generate_customers()
    agent_assignments = distribute_agents_to_managers(
        agents=agents,
        managers=managers,
        start_date=simulation_config.global_start_date,
        end_date=simulation_config.global_end_date,
    )
    agents.to_csv(f"{seed_output_dir}/agents.csv", index=False, header=True)
    managers.to_csv(f"{seed_output_dir}/managers.csv", index=False, header=True)
    customers.to_csv(f"{seed_output_dir}/customers.csv", index=False, header=True)
    agent_assignments.to_csv(f"{seed_output_dir}/agent_assignments.csv", index=False, header=True)

    call_id_counter, crm_id_counter, survey_id_counter = 0, 0, 0
    pending_callbacks = []

    customer_busy_until = {
        cid: datetime.datetime.combine(simulation_config.global_start_date, datetime.time.min)
        for cid in customers["customer_id"]
    }

    for day in pd.date_range(simulation_config.global_start_date, simulation_config.global_end_date):
        print(f"Simulation start for day: {day.date()}")
        print(f"calls: {call_id_counter}")
        print(f"crm: {crm_id_counter}")
        print(f"survey: {survey_id_counter}")
        day_date = day.date()
        day_in_month = day_date.day
        days_in_month = calendar.monthrange(day_date.year, day_date.month)[1]

        weekday_mult = simulation_config.weekday_multipliers[day_date.weekday()]
        seasonal_mult = 1 + simulation_config.seasonality_amplitude * np.cos(2 * np.pi * (day_in_month / days_in_month))
        volume_mult = weekday_mult * seasonal_mult

        day_calls, day_crm = [], []
        day_surveys_by_date = {}  # key = survey_date, value = list of surveys

        todays_callbacks = [cb for cb in pending_callbacks if cb["day"] == day_date]

        for _, agent_id in agents["agent_id"].items():
            n_calls = int(simulation_config.calls_per_agent_per_day * volume_mult)
            start_time = datetime.datetime.combine(day_date, datetime.time.min) + datetime.timedelta(
                hours=simulation_config.workday_start
            )

            agent_callbacks = [cb for cb in todays_callbacks if cb["agent_id"] == agent_id]
            work_items = ["new"] * n_calls + agent_callbacks
            rng.shuffle(work_items)

            for item in work_items:
                hold_time = simulate_hold_time(rng)
                inter_arrival = rng.exponential(scale=simulation_config.mean_seconds_between_calls)
                start_time = start_time + datetime.timedelta(seconds=hold_time + inter_arrival)

                if item != "new" and isinstance(item, dict):  # callback
                    customer_id = item["customer_id"]
                    customer = customers[customers["customer_id"] == customer_id]
                    reason, subreason = item["reason"], item["subreason"]
                    previous_issue_flag = rng.random() < simulation_config.previous_issue_rate
                    reason, subreason, duration = get_call_reasons_plus_duration(
                        simulation_config=simulation_config,
                        program_key=customer["program"].item(),
                        rng=rng,
                        reason=reason,
                        subreason=subreason,
                    )

                else:  # Select a free customer (fixed attempts)
                    customer = None
                    customer_id = None

                    for _ in range(15):
                        customer_idx = rng.integers(0, len(customers))
                        customer = customers.iloc[[customer_idx]]
                        customer_id = customer["customer_id"].item()

                        if start_time >= customer_busy_until[customer_id]:
                            break

                    if customer is None:
                        raise RuntimeError(f"No free customer found after 15 attempts at {start_time}")

                    reason, subreason, duration = get_call_reasons_plus_duration(
                        simulation_config=simulation_config,
                        program_key=customer["program"].item(),
                        rng=rng,
                    )
                    previous_issue_flag = False

                end_ts = start_time + datetime.timedelta(seconds=duration)
                customer_busy_until[customer_id] = end_ts

                # Stop work items if there isn't enough time in workday
                if end_ts.hour >= simulation_config.workday_end:
                    break

                transfer = rng.random() < simulation_config.transfer_rate

                hold_time_during_call = rng.integers(0, duration // 2, endpoint=True)

                # Telephony record
                call_id_counter += 1
                call_id = call_id_counter
                call = {
                    "call_id": call_id,
                    "agent_id": agent_id,
                    "customer_id": customer_id,
                    "queue_hold_time": hold_time,
                    "start_ts": start_time,
                    "end_ts": end_ts,
                    "duration_s": duration,
                    "hold_time_during_call_s": hold_time_during_call,
                    "transfer_flag": transfer,
                }

                day_calls.append(call)

                # CRM record
                crm_id_counter += 1
                crm = {
                    "crm_id": crm_id_counter,
                    "agent_id": agent_id,
                    "call_id": call_id,
                    "customer_id": customer_id,
                    "reason_code": reason,
                    "sub_reason_code": subreason,
                    "previous_issue_flag": previous_issue_flag,
                    "created_ts": start_time,
                }

                day_crm.append(crm)

                if rng.random() < simulation_config.survey_rate:
                    survey_id_counter += 1
                    response_ts = (
                        end_ts
                        + datetime.timedelta(seconds=float(rng.integers(15, 60, endpoint=True)))
                        + datetime.timedelta(days=float(rng.integers(0, 4, endpoint=True)))
                    )
                    csat = int(
                        np.clip(
                            rng.normal(4 if not transfer or hold_time_during_call < 60 else 2, 1),
                            a_min=1,
                            a_max=5,
                        )
                    )
                    nps = generate_nps(
                        rng=rng, transfer=transfer, hold_time=hold_time, previous_issue_flag=previous_issue_flag
                    )
                    survey = {
                        "survey_id": survey_id_counter,
                        "call_id": call_id,
                        "agent_id": agent_id,
                        "customer_id": customer_id,
                        "sent_ts": end_ts + datetime.timedelta(seconds=5),
                        "response_ts": response_ts,
                        "csat": csat,
                        "nps": nps,
                    }

                    day_surveys_by_date.setdefault(response_ts.date(), []).append(survey)

                if item == "new" and rng.random() < simulation_config.callback_rate:
                    days_out = int(rng.integers(1, 6, endpoint=True))
                    future_day = day_date + datetime.timedelta(days=days_out)
                    if future_day <= simulation_config.global_end_date:
                        other_agents = agents[agents["agent_id"] != agent_id]
                        cb_agent = other_agents.iloc[[rng.integers(0, len(other_agents))]]
                        # TODO: Sometimes use a different call reason/sub reason?
                        pending_callbacks.append({
                            "day": future_day,
                            "customer_id": customer_id,
                            "reason": reason,
                            "subreason": subreason,
                            "agent_id": cb_agent["agent_id"].item(),
                        })
                        # reserve the customer until the callback day starts (prevents being chosen before)
                        customer_busy_until[customer_id] = datetime.datetime.combine(future_day, datetime.time.min)
                # need to kick off the next call as another time after the duration of the call
                # so the inter-arrival time will get added next time start_time is reassigned
                start_time = end_ts

        # write daily calls/crm
        write_daily_parquet(records=day_calls, output_dir=parquet_output_dir, table="calls", date=day_date)
        write_daily_parquet(records=day_crm, output_dir=parquet_output_dir, table="crm", date=day_date)

        # write daily survey data based on response date
        # should get multiple files in directory because of the randomness of responses by day of call
        for survey_date, surveys in day_surveys_by_date.items():
            write_daily_parquet(records=surveys, output_dir=parquet_output_dir, table="surveys", date=survey_date)


def main(**overrides):
    """Run the call center simulation with optional config overrides."""
    config_overrides = {k: v for k, v in overrides.items() if k not in ["seed_output_dir", "parquet_output_dir"]}
    config = DEFAULT_CONFIG.with_overrides(**config_overrides) if config_overrides else DEFAULT_CONFIG

    sim_kwargs = {"simulation_config": config}
    if "seed_output_dir" in overrides:
        sim_kwargs["seed_output_dir"] = overrides["seed_output_dir"]
    if "parquet_output_dir" in overrides:
        sim_kwargs["parquet_output_dir"] = overrides["parquet_output_dir"]

    start_time = datetime.datetime.now()
    simulate_call_center(**sim_kwargs)
    end_time = datetime.datetime.now()

    print(f"COMPLETE: Simulation took {end_time - start_time} seconds.")


if __name__ == "__main__":
    main()


# OUTDATED: Going to model this so that dbt snapshots the value for the manager column
# using the "check" strategy because I don't want to manage shuffling/shifting the updated_at times.
# That will just be done when hydrating each particular assignment/exercise.

# Snapshots will be harder to implement because they use the actual runtime date to establish to effective dates
# So I'll need to generate the agent/manager relationship separately.
