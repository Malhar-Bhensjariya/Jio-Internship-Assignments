import os
from datetime import datetime, time, timezone
from google.cloud import monitoring_v3
import pytz

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT is not set.")

VM_NAMES = ['lbtcp-vm1', 'lbtcp-vm2', 'lbtcp-vm3']

local_tz = pytz.timezone('Asia/Kolkata')

client = monitoring_v3.MetricServiceClient()
project_name = f"projects/{PROJECT_ID}"

def get_user_datetime_range():
    date_format = "%d-%m-%Y"
    time_format = "%H:%M"

    start_date_str = input("Enter start date (DD-MM-YYYY): ")
    end_date_str = input("Enter end date (DD-MM-YYYY): ")

    try:
        start_date = datetime.strptime(start_date_str, date_format).date()
        end_date = datetime.strptime(end_date_str, date_format).date()
    except ValueError:
        raise ValueError("Invalid date format. Please use DD-MM-YYYY.")

    use_custom_time = input("Do you want to specify time as well? (y/n): ").strip().lower() == 'y'

    if use_custom_time:
        print("Enter time in 24-hour format.\n")
        start_time_str = input("Enter start time (HH:MM): ")
        end_time_str = input("Enter end time (HH:MM): ")
        try:
            start_time_obj = datetime.strptime(start_time_str, time_format).time()
            end_time_obj = datetime.strptime(end_time_str, time_format).time()
        except ValueError:
            raise ValueError("Invalid time format. Please use HH:MM.")
    else:
        print("Default time taken 00:00 - 23:59")
        start_time_obj = time(0, 0)
        end_time_obj = time(23, 59)

    start_local = local_tz.localize(datetime.combine(start_date, start_time_obj))
    end_local = local_tz.localize(datetime.combine(end_date, end_time_obj))

    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def fetch_vm_cpu_utilization_raw(vm_name, start_time, end_time):
    interval = monitoring_v3.TimeInterval({
        "start_time": {"seconds": int(start_time.timestamp())},
        "end_time": {"seconds": int(end_time.timestamp())},
    })

    filter_str = (
        f'metric.type="compute.googleapis.com/instance/cpu/utilization" AND '
        f'metric.labels.instance_name="{vm_name}"'
    )

    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": filter_str,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )

    raw_data_points = []
    for series in results:
        for point in series.points:
            ts = point.interval.end_time.replace(tzinfo=timezone.utc)
            if ts < start_time or ts > end_time:
                continue
            value = point.value.double_value * 100 
            if value <= 100:
                raw_data_points.append(value)
    return raw_data_points

def compute_stats(data):
    if not data:
        return None
    sorted_data = sorted(data)
    n = len(sorted_data)
    median = sorted_data[n//2] if n % 2 == 1 else (sorted_data[n//2-1] + sorted_data[n//2])/2
    return min(data), max(data), median, len(data)

def main():
    start_time, end_time = get_user_datetime_range()
    vm_stats = {}
    all_data = []

    # Collect and process data for each VM
    for vm in VM_NAMES:
        data = fetch_vm_cpu_utilization_raw(vm, start_time, end_time)
        if data:
            vm_stats[vm] = compute_stats(data)
            all_data.extend(data)

    # Compute combined statistics
    combined_stats = compute_stats(all_data) if all_data else None

    # Print summary table
    print("\n===== CPU Utilization Summary (Raw Unaggregated Data) =====")
    print(f"{'VM Name':<12} {'Min (%)':>10} {'Max (%)':>10} {'Median (%)':>12} {'Samples':>10}")
    print("-" * 70)
    
    # Individual VM stats
    for vm in VM_NAMES:
        stats = vm_stats.get(vm)
        if not stats:
            print(f"{vm:<12} {'No Data':>10}")
            continue
        min_val, max_val, median_val, samples = stats
        print(f"{vm:<12} {min_val:10.2f} {max_val:10.2f} {median_val:12.3f} {samples:10}")

    # Combined stats
    if combined_stats:
        min_val, max_val, median_val, samples = combined_stats
        print("-" * 70)
        print(f"{'Combined':<12} {min_val:10.2f} {max_val:10.2f} {median_val:12.3f} {samples:10}")

if __name__ == "__main__":
    main()
