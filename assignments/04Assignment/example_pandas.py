import pandas as pd
import time
from memory_profiler import profile

# Load CSVs into pandas DataFrames
registrations_df = pd.read_csv('./strum_comp_X/registrations.csv').dropna()
payments_df = pd.read_csv('./strum_comp_X/payments.csv').dropna()
act_df = pd.read_csv('./strum_comp_X/activities.csv').dropna()
ses_df = pd.read_csv('./strum_comp_X/sessions.csv').dropna()

@profile
def find_partition_of_sessions_by_payments():
    start = time.time()

    # Aggregate total purchase rounded by user_id in payments_df
    paying_users_to_sum_paid = payments_df.groupby('user_id')['amount_usd'].sum().reset_index()
    paying_users_to_sum_paid['total_purchase_rounded'] = paying_users_to_sum_paid['amount_usd'].round()

    # Count session_number for each user_id in sessions_df
    paying_users_to_session_count = ses_df.groupby('user_id')['session_number'].count().reset_index()
    paying_users_to_session_count.rename(columns={'session_number': 'session_count'}, inplace=True)

    # Merge the aggregated payments with the session counts on 'user_id'
    paying_users_to_session_count_with_sum_window = paying_users_to_sum_paid.merge(
        paying_users_to_session_count,
        on='user_id',
        how='inner'
    )

    # Add the 'money_window' column (total_purchase_rounded // 5)
    paying_users_to_session_count_with_sum_window['money_window'] = paying_users_to_session_count_with_sum_window[
                                                                        'total_purchase_rounded'] // 5

    # Group by 'money_window' and calculate the average session count per user per window
    payment_sum_to_session_distribution = paying_users_to_session_count_with_sum_window.groupby('money_window')[
        'session_count'].mean().reset_index()
    payment_sum_to_session_distribution.rename(columns={'session_count': 'avg_session_count_per_user_per_window'},
                                               inplace=True)

    # Sort the results by 'money_window'
    payment_sum_to_session_distribution.sort_values(by='money_window', ascending=False, inplace=True)
    end = time.time()
    # Print the resulting dataframe
    print(f"The time: {end-start}\n{payment_sum_to_session_distribution}")


find_partition_of_sessions_by_payments()
