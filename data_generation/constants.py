import datetime

GLOBAL_START_DATE = datetime.date(2025, 1, 1)
GLOBAL_END_DATE = datetime.date(2025, 3, 31)

# For modeling seasonality
WEEKDAY_MULTIPLIERS = {
    0: 1.2,  # Monday
    1: 1.1,
    2: 1.0,
    3: 1.0,
    4: 1.1,
    5: 0.8,  # Saturday
    6: 0.7,  # Sunday
}

### Establish Programs/Call Type

# programs represents different internal business groupings
# these will influence call volumes/lengths/call reasons

PROGRAMS = {
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