import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import yaml


def update_times(df: pd.DataFrame, time_factor: float) -> pd.DataFrame:
    """
    Update the start and end times to present day factored by the time factor
    """

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for i, row in df.iterrows():
        device_type, device_name, action_type, action_value, start_time, end_time = row

        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor

        new_start_time = datetime.now(UTC)

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


def yaml_action_to_df(action: dict, observatory: str) -> pd.DataFrame:
    """
    Convert a YAML action to a DataFrame row
    """
    assert len(action) == 1, "action must have a single key containing the action type"
    action = unflatten_action(action)
    action_type = list(action.keys())[0]
    device_type = "Camera"
    device_name = f"camera_{observatory}"
    start_time = action[action_type].get("start_time", None)
    end_time = action[action_type].get("end_time", None)

    # collect the action values
    action_value = {
        key: value
        for key, value in action[action_type].items()
        if not key in ["start_time", "end_time"]
    }

    action_value = json.dumps(action_value)

    return pd.DataFrame(
        [[device_type, device_name, action_type, action_value, start_time, end_time]],
        columns=[
            "device_type",
            "device_name",
            "action_type",
            "action_value",
            "start_time",
            "end_time",
        ],
    )


def read_schedule(filename, observatory=None, truncate=False) -> pd.DataFrame:
    schedule_path = Path(filename)

    if schedule_path.exists() is False:
        raise FileNotFoundError(f"File not found: {filename}")

    if observatory is None:
        observatory = schedule_path.stem

    # read schedule and convert to a DataFrame
    if schedule_path.suffix == ".csv":
        schedule = pd.read_csv(schedule_path)
    elif schedule_path.suffix in [".yml", ".yaml"]:
        schedule = pd.DataFrame()
        with open(schedule_path, "r") as f:
            schedule_list = yaml.safe_load(f)
            for action in schedule_list:
                schedule = pd.concat(
                    [
                        schedule,
                        yaml_action_to_df(action, observatory=observatory),
                    ],
                    ignore_index=True,
                )
    else:
        schedule

    # at this point schedule must be a DataFrame
    schedule["start_time"] = pd.to_datetime(schedule.start_time)
    schedule["end_time"] = pd.to_datetime(schedule.end_time)

    # sort the schedule by start_time
    schedule = schedule.sort_values(by=["start_time"])

    # for development: Truncate the schedule if self.truncate_schedule is True
    if truncate:
        schedule = update_times(schedule, 10)

    schedule["completed"] = False

    return schedule


# temporary if we keep the csv thing
def action_series_to_dict(row: pd.Series) -> dict:
    """
    Convert a DataFrame row to a dictionary
    """
    action = {}
    action[row["action_type"]] = {
        "start_time": row["start_time"],
        "end_time": row["end_time"],
    }

    action_value_dict = eval(row["action_value"].replace("'", '"'))

    if len(action_value_dict) > 0:
        action[row["action_type"]].update(
            {
                key: value
                for key, value in action_value_dict.items()
                if not key in ["start_time", "end_time"]
            }
        )

    return action


def unflatten_action(action: dict) -> dict:
    """
    Unflatten a flat action value for filter and n
    """
    action_type = list(action.keys())[0]
    action_value = action[action_type]

    if action_type in ["flats", "calib"]:
        if isinstance(action_value["filter"], str):
            action_value["filter"] = [action_value["filter"]]
            action_value["n"] = [action_value["n"]]

    return {action_type: action_value}


def check_action(action: dict) -> bool:
    """
    Check if an action is valid
    """
    message = None

    if len(action) == 1:
        message = "action must have a single key containing the action type"
    action_type = list(action.keys())[0]

    if not action_type in [
        "open",
        "close",
        "object",
        "flats",
        "calibration",
    ]:
        message = "action type must be one of 'open', 'close', 'object', 'flats', 'calibration'"

    action_value = action[action_type]

    if action_type in ["flats", "calib"]:
        if not "filter" in action_value:
            message = "filter must be specified"
        if not "n" in action_value:
            message = "n must be specified"
        if not "start_time" in action_value:
            message = "start_time must be specified"
        if not "end_time" in action_value:
            message = "end_time must be specified"

    if message is None:
        return True, "Valid schedule action"
    else:
        return False, message
