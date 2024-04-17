from datetime import datetime

import pandas as pd
import yaml


def update_times(df, time_factor):
    """
    Update the start and end times to present day factored by the time factor
    """

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for _, row in df.iterrows():
        device_type, device_name, action_type, action_value, start_time, end_time = row

        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor

        new_start_time = datetime.now(datetime.UTC)

        if prev_end_time:
            ss_time_diff = start_time - prev_start_time
            ss_time_diff = ss_time_diff / time_factor

            new_start_time = prev_new_start_time + ss_time_diff

        new_end_time = new_start_time + se_time_diff

        new_row = [
            device_type,
            device_name,
            action_type,
            action_value,
            new_start_time,
            new_end_time,
        ]
        new_rows.append(new_row)

        prev_start_time = start_time
        prev_end_time = end_time

        prev_new_start_time = new_start_time

    return pd.DataFrame(new_rows, columns=df.columns)


def read_schedule_file(file_path, truncate=False) -> pd.DataFrame:
    """
    Read the schedule from the csv or yaml file
    """
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".yaml") or file_path.endswith(".yml"):
        data = yaml.load(open(file_path, "r"), Loader=yaml.FullLoader)
        df = pd.DataFrame(data)
    else:
        raise ValueError(f"File format not supported: {file_path}")

    # TODO: Checks?

    df = pd.read_csv(file_path)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    # Sort the schedule by start_time
    df = df.sort_values(by=["start_time"])
    if truncate:
        df = update_times(df, 10)

    df["completed"] = False

    return df
