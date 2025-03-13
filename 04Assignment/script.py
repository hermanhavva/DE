import polars as pl
import time
from memory_profiler import profile

registrations_df = pl.read_csv('./strum_comp_X/registrations.csv').drop_nulls()
payments_df = pl.read_csv('./strum_comp_X/payments.csv').drop_nulls()
act_df = pl.read_csv('./strum_comp_X/activities.csv').drop_nulls()
ses_df = pl.read_csv('./strum_comp_X/sessions.csv').drop_nulls()

# define non-paying users and the avg number of sessions for them
# sum payments for each user and then for each sum (with a step of 5 dollars)
# of money define average amount of sessions (and likes optionally)


@profile
def find_non_pay_user_count_and_avg_session_count_of_non_pay_user():
    non_paying_user_id_df = pl.DataFrame(registrations_df.join(payments_df, how='left', on='user_id')['user_id'])

    session_count_per_non_paying_user = (non_paying_user_id_df
                                         .join(ses_df, how='inner', on='user_id')
                                         .group_by('user_id')
                                         .agg([pl.col('session_number').count().alias('session_count')]))
    # print(session_count_per_non_paying_user)

    non_pay_user_count_to_avg_non_pay_ses_count = (session_count_per_non_paying_user
                                                   .select([pl.col('user_id')
                                                           .count()
                                                           .alias('non_paying_user'), pl.col('session_count')
                                                           .mean()
                                                           .alias('session_count_avg')]))

    print(f"Count of non-paying users to avg session count:\n {non_pay_user_count_to_avg_non_pay_ses_count}")


@profile
def find_partition_of_sessions_by_payments():
    start = time.time()
    paying_users_to_sum_paid = (payments_df
                                .select(pl.col('user_id'), pl.col('amount_usd'))
                                .group_by(pl.col('user_id'))
                                .agg([pl.col('amount_usd').sum().round().alias('total_purchase_rounded')]))

    paying_users_to_session_count = (paying_users_to_sum_paid
                                     .join(ses_df
                                           .group_by(pl.col('user_id')).agg([pl.col('session_number').count().alias(('session_count'))]),
                                           on='user_id',
                                           how='inner'))

    paying_users_to_session_count_with_sum_window = (paying_users_to_session_count
                                                     .with_columns((pl.col('total_purchase_rounded') // 5)
                                                                   .alias('money_window')))

    # print(paying_users_to_session_count_with_sum_window)

    payment_sum_to_session_distribution = (paying_users_to_session_count_with_sum_window
                                           .group_by('money_window')  # group by the 'money_window'
                                           .agg([pl.col('session_count').mean().alias('avg_session_count_per_user_per_window')])
                                           .sort('money_window'))
    end = time.time()
    print(f"The time: {end - start}\n{payment_sum_to_session_distribution}")

# find_non_pay_user_count_and_avg_session_count_of_non_pay_user()
# find_partition_of_sessions_by_payments()
