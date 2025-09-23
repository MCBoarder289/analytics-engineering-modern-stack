import datetime
import os
import random
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

MIN_CALL_LENGTH = 20  # seconds

### Establish Programs/Call Type

# programs represents different internal business groupings
# these will influence call volumes/lengths/call reasons

programs = {
    "Technical Support": {
        "reasons": [
            {
                "name": "Device Issue",
                "prob": 0.5,
                "sub_reasons": [
                    {"name": "Phone", "prob": 0.55, "duration_mean": 300, "duration_std": 60},
                    {"name": "Laptop", "prob": 0.10, "duration_mean": 600, "duration_std": 90},
                    {"name": "TV", "prob": 0.35, "duration_mean": 700, "duration_std": 120},
                ]
            },
            {
                "name": "Login Issue",
                "prob": 0.4,
                "sub_reasons": [
                    {"name": "Forgot Password", "prob": 0.8, "duration_mean": 180, "duration_std": 40},
                    {"name": "Account Expired", "prob": 0.2, "duration_mean": 250, "duration_std": 60},
                ]
            },
            {
                "name": "Other Issue",
                "prob": 0.1,
                "sub_reasons": [
                    {"name": "Billing Questions", "prob": 0.75, "duration_mean": 200, "duration_std": 45},
                    {"name": "General Inquiry", "prob": 0.25, "duration_mean": 120, "duration_std": 20 },
                ]
            }
        ]
    },
    "Claim Administration": {
        "reasons": [
            {
                "name": "New Claim",
                "prob": 0.4,
                "sub_reasons": [
                    {"name": "Medical", "prob": 0.5, "duration_mean": 900, "duration_std": 60},
                    {"name": "Auto", "prob": 0.3, "duration_mean": 720, "duration_std": 35},
                    {"name": "Other", "prob": 0.2, "duration_mean": 600, "duration_std": 120},
                ]
            },
            {
                "name": "Claim Status",
                "prob": 0.6,
                "sub_reasons": [
                    {"name": "Pending", "prob": 0.5, "duration_mean": 480, "duration_std": 120},
                    {"name": "Approved", "prob": 0.3, "duration_mean": 360, "duration_std": 120},
                    {"name": "Denied", "prob": 0.2, "duration_mean": 720, "duration_std": 240},
                ]
            }
        ]
    },
    "Financial Planning": {
        "reasons": [
            {
                "name": "Budget Help",
                "prob": 0.5,
                "sub_reasons": [
                    {"name": "Monthly Plan", "prob": 0.7, "duration_mean": 900, "duration_std": 200},
                    {"name": "Annual Plan", "prob": 0.3, "duration_mean": 1200, "duration_std": 300},
                ]
            },
            {
                "name": "Investment Advice",
                "prob": 0.5,
                "sub_reasons": [
                    {"name": "Retirement", "prob": 0.5, "duration_mean": 1800, "duration_std": 600},
                    {"name": "Stocks", "prob": 0.5, "duration_mean": 1500, "duration_std": 720},
                ]
            }
        ]
    }
}

GLOBAL_START_DATE = datetime.date(2025, 1, 1)
GLOBAL_END_DATE = datetime.date(2025, 3, 31)

### Helper function to simulate call reasons
def get_call_reasons_plus_duration(program_key: str, reason: str | None = None, subreason: str | None = None):
    if reason and subreason:
        reason_record = [
            reason_record
            for reason_record in programs[program_key]["reasons"]
            if reason_record["name"] == reason
        ][0]
        subreason_record = [
            subreason_record
            for subreason_record in reason_record["sub_reasons"]
            if subreason_record["name"] == subreason
        ][0]

        duration = max(
            MIN_CALL_LENGTH, int(np.random.normal(subreason_record["duration_mean"], subreason_record["duration_std"]))
        )

        return reason, subreason, duration
    else:
        reasons = programs[program_key]["reasons"]
        reason_probs = [r["prob"] for r in reasons]

        reason = np.random.choice(reasons, p=reason_probs)
        sub_reasons = reason["sub_reasons"]
        sub_probs = [s["prob"] for s in sub_reasons]

        sub_reason = np.random.choice(sub_reasons, p=sub_probs)

        # duration draw ~ Normal(mean, std)
        duration = max(MIN_CALL_LENGTH, int(np.random.normal(sub_reason["duration_mean"], sub_reason["duration_std"])))

        return reason["name"], sub_reason["name"], duration


### Generate Customer Records

# Customer will get a single program
def generate_customers(
    num_customers: int,
) -> pd.DataFrame:

    fake = Faker()
    Faker.seed(289)
    random.seed(315)

    program_names = list(programs.keys())

    customer_records = []

    for i in range(num_customers):
        state = fake.state_abbr(include_territories=False, include_freely_associated_states=False)
        program_selection = random.choice(program_names)
        customer_record = {
            'customer_id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'birth_date': fake.date_between(start_date="-77y", end_date="-21y"),
            'state': state,
            'zip_code': fake.zipcode_in_state(state_abbr=state),
            'program': program_selection
        }

        customer_records.append(customer_record)

    customer_df = pd.DataFrame.from_records(customer_records)
    customer_df["zip_code"] = customer_df["zip_code"].astype(str)

    return customer_df

# test = generate_customers(num_customers=1500)
# test2 = generate_customers(num_customers=1500)
#
# equals = test.equals(test2)

### Generate Agent Records

def generate_agents(num_agents:int) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(867)

    agent_records = []

    for i in range(num_agents):
        agent_record = {
            'agent_id': i + 1,
            'agent_name': fake.name(),
        }
        agent_records.append(agent_record)

    return pd.DataFrame.from_records(agent_records)

def generate_managers(num_managers: int) -> pd.DataFrame:
    fake = Faker()
    Faker.seed(222)

    manager_records = []

    for i in range(num_managers):
        manager_record = {
            'manager_id': i + 1,
            'manager_name': fake.name(),
        }
        manager_records.append(manager_record)

    return pd.DataFrame.from_records(manager_records)


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
            assignments.append(
                {
                    "agent_id": agent["agent_id"],
                    "manager_id": manager_id,
                    "effective_start": current_start,
                    "effective_end": change_date - datetime.timedelta(days=1),
                }
            )

            # switch manager, exclude current
            other_mgrs = managers["manager_id"].loc[managers["manager_id"] != manager_id].to_numpy()
            manager_id = rng.choice(other_mgrs)

            current_start = change_date

        # final segment until end_date
        assignments.append(
            {
                "agent_id": agent["agent_id"],
                "manager_id": manager_id,
                "effective_start": current_start,
                "effective_end": pd.to_datetime(end_date),
            }
        )

    assignments_df = pd.DataFrame(assignments)
    mask = assignments_df['effective_end'] == pd.to_datetime(end_date)
    assignments_df.loc[mask, 'effective_end'] = pd.NaT

    return assignments_df

agents = generate_agents(num_agents=50)
managers = generate_managers(num_managers=5)
agent_assignments = distribute_agents_to_managers(
    agents=agents,
    managers=managers,
    start_date=GLOBAL_START_DATE,
    end_date=GLOBAL_END_DATE,
)
customers = generate_customers(num_customers=1500)

## Write out customers, managers, and assignments csv files for dbt seed
agents.to_csv("../call_center/seeds/agents.csv", index=False, header=True)
managers.to_csv("../call_center/seeds/managers.csv", index=False, header=True)
agent_assignments.to_csv("../call_center/seeds/agent_assignments.csv", index=False, header=True)
customers.to_csv("../call_center/seeds/customers.csv", index=False, header=True)


### FULL SIMULATION
CALLS_PER_AGENT_PER_DAY = 60  # Likely not enough
CALLBACK_RATE = 0.1
SURVEY_RATE = 0.3
PREVIOUS_ISSUE_RATE = 0.4
TRANSFER_RATE = 0.1
WORKDAY_START = 8
WORKDAY_END = 17
MEAN_SECONDS_BETWEEN_CALLS = 600

# Seasonality + weekday scaling
SEASONALITY_AMPLITUDE = 0.3  # +/- 30%
WEEKDAY_MULTIPLIERS = {
    0: 1.2,  # Monday
    1: 1.1,
    2: 1.0,
    3: 1.0,
    4: 1.1,
    5: 0.8,  # Saturday
    6: 0.7,  # Sunday
}

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


def simulate_call_center(rng: np.random.Generator):
    call_id_counter, crm_id_counter, survey_id_counter = 0, 0, 0
    pending_callbacks = []

    for day in pd.date_range(GLOBAL_START_DATE, GLOBAL_END_DATE):
        print(f"Simulation start for day: {day}")
        print(f"calls: {call_id_counter}")
        print(f"crm: {crm_id_counter}")
        print(f"survey: {survey_id_counter}")
        day_date = day.date()
        weekday_mult = WEEKDAY_MULTIPLIERS[day_date.weekday()]
        seasonal_mult = 1 + SEASONALITY_AMPLITUDE * np.sin(2 * np.pi * (day_date - GLOBAL_START_DATE).days / 30)
        volume_mult = weekday_mult * seasonal_mult

        day_calls, day_crm = [], []
        day_surveys_by_date = {}  # key = survey_date, value = list of surveys

        todays_callbacks = [cb for cb in pending_callbacks if cb["day"] == day_date]

        for _, agent_id in agents["agent_id"].items():
            print(f"Agent ID: {agent_id}")
            n_calls = int(CALLS_PER_AGENT_PER_DAY * volume_mult)
            start_time = datetime.datetime.combine(day_date, datetime.datetime.min.time()) + datetime.timedelta(
                hours=WORKDAY_START
            )

            agent_callbacks = [cb for cb in todays_callbacks if cb["agent_id"] == agent_id]
            work_items = ["new"] * n_calls + agent_callbacks
            rng.shuffle(work_items)

            for item in work_items:
                if item != "new" and isinstance(item, dict):  # callback
                    customer = customers[customers["customer_id"] == item["customer_id"]]
                    reason, subreason = item["reason"], item["subreason"]
                    previous_issue_flag = rng.random() < PREVIOUS_ISSUE_RATE
                    reason, subreason, duration = get_call_reasons_plus_duration(
                        program_key=customer["program"].item(), reason=reason, subreason=subreason
                    )
                else:
                    customer = customers.sample(1, random_state=rng)
                    reason, subreason, duration = get_call_reasons_plus_duration(program_key=customer["program"].item())
                    previous_issue_flag = False

                hold_time = simulate_hold_time(rng)
                inter_arrival = rng.exponential(scale=MEAN_SECONDS_BETWEEN_CALLS)
                start_time = start_time + datetime.timedelta(seconds=hold_time + inter_arrival)
                end_ts = start_time + datetime.timedelta(seconds=duration)

                # Stop work items if there isn't enough time in workday
                if end_ts.hour >= WORKDAY_END:
                    break

                transfer = rng.random() < TRANSFER_RATE

                # Telephony record
                call_id_counter += 1
                call_id = call_id_counter
                call = {
                    "call_id": call_id,
                    "agent_id": agent_id,
                    "customer_id": customer["customer_id"].item(),
                    "queue_hold_time": hold_time,
                    "start_ts": start_time,
                    "end_ts": end_ts,
                    "duration_s": duration,
                    "hold_time_during_call_s": rng.integers(0,60),
                    "transfer_flag": transfer,
                }

                day_calls.append(call)

                # CRM record
                crm_id_counter += 1
                crm = {
                    "crm_id": crm_id_counter,
                    "agent_id": agent_id,
                    "call_id": call_id,
                    "customer_id": customer["customer_id"].item(),
                    "reason_code": reason,
                    "sub_reason_code": subreason,
                    "previous_issue_flag": previous_issue_flag,
                    "created_ts": start_time,
                }

                day_crm.append(crm)

                if rng.random() < SURVEY_RATE:
                    survey_id_counter += 1
                    response_ts = (
                        end_ts
                        + datetime.timedelta(seconds=float(rng.integers(15, 60)))
                        + datetime.timedelta(days=float(rng.integers(0, 4)))
                    )
                    csat = int(np.clip(rng.normal(4 if not transfer and hold_time < 60 else 3, 1), 1, 5))
                    nps = int(
                        np.clip(
                            rng.normal(7 if not transfer and hold_time < 60 and not previous_issue_flag else 5, 1.5),
                            a_min=1,
                            a_max=10
                        )
                    )
                    survey = {
                        "survey_id": survey_id_counter,
                        "call_id": call_id,
                        "agent_id": agent_id,
                        "customer_id": customer["customer_id"].item(),
                        "sent_ts": end_ts + datetime.timedelta(seconds=5),
                        "response_ts": response_ts,
                        "csat": csat,
                        "nps": nps,
                    }

                    day_surveys_by_date.setdefault(response_ts.date(), []).append(survey)

                if item == "new" and rng.random() < CALLBACK_RATE:
                    days_out = int(rng.integers(1, 6))
                    future_day = day_date + datetime.timedelta(days=days_out)
                    if future_day < GLOBAL_END_DATE:
                        cb_agent = agents[agents["agent_id"] != agent_id].sample(1)
                        pending_callbacks.append({
                            "day": future_day,
                            "customer_id": customer["customer_id"].item(),
                            "reason": reason,
                            "subreason": subreason,
                            "agent_id": cb_agent["agent_id"].item(),
                        })
                # need to kick off the next call as another time after the duration of the call
                # so the inter-arrival time will get added next time start_time is reassigned
                start_time = end_ts

        # write daily calls/crm
        write_daily_parquet(records=day_calls, output_dir="../data", table="calls", date=day_date)
        write_daily_parquet(records=day_crm, output_dir="../data", table="crm", date=day_date)

        # write daily survey data based on response date
        # should get multiple files in directory because of the randomness of responses by day of call
        for survey_date, surveys in day_surveys_by_date.items():
            write_daily_parquet(records=surveys, output_dir="../data", table="surveys", date=survey_date)


simulate_call_center(rng=np.random.default_rng(289))


# OUTDATED: Going to model this so that dbt snapshots the value for the manager column
# using the "check" strategy because I don't want to manage shuffling/shifting the updated_at times.
# That will just be done when hydrating each particular assignment/exercise.

# Snapshots will be harder to implement because they use the actual runtime date to establish to effective dates
# So I'll need to generate the agent/manager relationship separately.



