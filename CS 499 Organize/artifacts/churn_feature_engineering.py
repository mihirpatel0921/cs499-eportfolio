# churn_feature_engineering.py

import pandas as pd
import numpy as np

def engineer_features(df):
    # Calculate average monthly charge, safely handling tenure = 0
    df['average_monthly_charge'] = df['totalcharges'] / df['tenure'].replace(0, 1)

    # Define tenure buckets
    tenure_buckets = {
        'New': (0, 12),
        'Established': (13, 36),
        'Loyal': (37, 72)
    }

    # Assign customers into tenure buckets
    def assign_bucket(tenure):
        for label, (low, high) in tenure_buckets.items():
            if low <= tenure <= high:
                return label
        return 'Veteran'

    df['tenure_bucket'] = df['tenure'].apply(assign_bucket)

    # Create a binary flag for tech support usage
    df['tech_support_flag'] = np.where(df['techsupport'] == 'Yes', 1, 0)

    # Derive churn risk score
    df['churn_risk_score'] = df['monthlycharges'] * df['tech_support_flag']

    return df
