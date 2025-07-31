import logging
import os
import sys
from datetime import datetime, time, timezone
import pytz
from google.cloud import monitoring_v3
from googleapiclient import discovery
from google.auth import default
from tabulate import tabulate

# Suppress noisy logs
logging.basicConfig(level=logging.ERROR)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('httplib2').setLevel(logging.ERROR)

# Environment variables
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
ZONE = os.getenv('ZONE') or os.getenv('GOOGLE_CLOUD_ZONE')

if not PROJECT_ID:
    raise EnvironmentError("GOOGLE_CLOUD_PROJECT is not set.")
if not ZONE:
    raise EnvironmentError("ZONE or GOOGLE_CLOUD_ZONE is not set.")

local_tz = pytz.timezone('Asia/Kolkata')

# Initialize clients
credentials, _ = default()
monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)
compute_client = discovery.build('compute', 'v1', credentials=credentials)
project_name = f"projects/{PROJECT_ID}"

def get_vms_with_details(label_key, label_value):
    filter_str = f"(labels.{label_key} = {label_value})"
    request = compute_client.instances().list(project=PROJECT_ID, zone=ZONE, filter=filter_str)
    instances = []
    while request:
        response = request.execute()
        for inst in response.get('items', []):
            instances.append({
                'name': inst['name'],
                'id': inst['id'],
                'zone': inst['zone'].split('/')[-1]
            })
        request = compute_client.instances().list_next(request, response)
    return instances

def get_user_datetime_range():
    date_fmt = "%d-%m-%Y"
    time_fmt = "%H:%M"

    while True:
        try:
            start_date_str = input("Enter start date (DD-MM-YYYY): ").strip()
            end_date_str = input("Enter end date (DD-MM-YYYY): ").strip()
            start_date = datetime.strptime(start_date_str, date_fmt).date()
            end_date = datetime.strptime(end_date_str, date_fmt).date()
            if end_date < start_date:
                print("End date cannot be before start date.")
                continue
            break
        except ValueError:
            print("Invalid date format. Use DD-MM-YYYY.")

    use_time = input("Specify time as well? (y/n): ").strip().lower() == 'y'
    if use_time:
        while True:
            try:
                start_time_str = input("Start time (HH:MM): ").strip()
                end_time_str = input("End time (HH:MM): ").strip()
                start_time_obj = datetime.strptime(start_time_str, time_fmt).time()
                end_time_obj = datetime.strptime(end_time_str, time_fmt).time()
                break
            except ValueError:
                print("Invalid time format. Use HH:MM.")
    else:
        start_time_obj = time(0, 0)
        end_time_obj = time(23, 59)

    start_local = local_tz.localize(datetime.combine(start_date, start_time_obj))
    end_local = local_tz.localize(datetime.combine(end_date, end_time_obj))
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def fetch_cpu_utilization(vm_name, vm_id, start_time, end_time):
    interval = monitoring_v3.TimeInterval({
        "start_time": {"seconds": int(start_time.timestamp())},
        "end_time": {"seconds": int(end_time.timestamp())},
    })

    filter_str = (
        f'metric.type="compute.googleapis.com/instance/cpu/utilization" AND '
        f'resource.labels.instance_id="{vm_id}"'
    )

    try:
        results = monitoring_client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            }
        )

        values = []
        for series in results:
            for point in series.points:
                val = point.value.double_value * 100
                if 0 <= val <= 100:
                    values.append(val)

        return values

    except Exception as e:
        print(f"Error fetching CPU for {vm_name}: {e}")
        return []

def fetch_memory_utilization(vm_name, vm_id, start_time, end_time):
    interval = monitoring_v3.TimeInterval({
        "start_time": {"seconds": int(start_time.timestamp())},
        "end_time": {"seconds": int(end_time.timestamp())},
    })

    filter_str = (
        f'metric.type="agent.googleapis.com/memory/percent_used" AND '
        f'resource.labels.instance_id="{vm_id}" AND '
        f'metric.labels.state="used"'
    )

    try:
        results = monitoring_client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
            }
        )

        values = []
        for series in results:
            for point in series.points:
                val = point.value.double_value
                if val <= 1.0:
                    val *= 100
                if 0 <= val <= 100:
                    values.append(val)

        return values

    except Exception as e:
        print(f"Error fetching memory for {vm_name}: {e}")
        return []

def fetch_disk_utilization(vm_name, vm_id, start_time, end_time, device=None):
    interval = monitoring_v3.TimeInterval({
        "start_time": {"seconds": int(start_time.timestamp())},
        "end_time": {"seconds": int(end_time.timestamp())},
    })

    base_filter = (
        f'metric.type="agent.googleapis.com/disk/percent_used" AND '
        f'resource.labels.instance_id="{vm_id}"'
    )

    if device:
        base_filter += f' AND metric.labels.device="{device}"'

    base_filter += ' AND metric.labels.state="used"'

    aggregation = {
        "alignment_period": {"seconds": 60},
        "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
    }

    try:
        results = monitoring_client.list_time_series(
            request={
                "name": project_name,
                "filter": base_filter,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
            }
        )

        values = []
        for series in results:
            for point in series.points:
                val = point.value.double_value
                if val <= 1.0:
                    val *= 100
                if 1.0 <= val <= 99.0:
                    values.append(val)

        return values

    except Exception as e:
        print(f"Error fetching disk for {vm_name} device {device or 'all'}: {e}")
        return []

def compute_stats(data):
    if not data:
        return None

    sorted_data = sorted(data)
    n = len(sorted_data)
    median = sorted_data[n // 2] if n % 2 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
    mean = sum(data) / n
    return min(data), max(data), median, mean, n

def main():
    label_key = input("Enter label key (e.g., app): ").strip()
    label_value = input("Enter label value (e.g., lbtcp): ").strip()

    if not label_key or not label_value:
        print("Label key and value cannot be empty.")
        sys.exit(1)

    vms = get_vms_with_details(label_key, label_value)
    if not vms:
        print(f"No VMs found with label {label_key}={label_value} in zone {ZONE}")
        sys.exit(0)

    vm_names = [vm['name'] for vm in vms]
    print(f"Found VMs: {', '.join(vm_names)}")

    start_time, end_time = get_user_datetime_range()
    print(f"Fetching metrics from {start_time} to {end_time} (UTC)")

    # Data containers for combined stats
    all_cpu_data = []
    all_memory_data = []
    all_disk_total_data = []
    all_disk_sda1_data = []
    all_disk_sda15_data = []

    for vm in vms:
        vm_name = vm['name']
        vm_id = vm['id']

        print(f"\nProcessing {vm_name}...")

        cpu_data = fetch_cpu_utilization(vm_name, vm_id, start_time, end_time)
        memory_data = fetch_memory_utilization(vm_name, vm_id, start_time, end_time)
        disk_total = fetch_disk_utilization(vm_name, vm_id, start_time, end_time)
        disk_sda1 = fetch_disk_utilization(vm_name, vm_id, start_time, end_time, device="/dev/sda1")
        disk_sda15 = fetch_disk_utilization(vm_name, vm_id, start_time, end_time, device="/dev/sda15")

        # Accumulate combined data
        all_cpu_data.extend(cpu_data)
        all_memory_data.extend(memory_data)
        all_disk_total_data.extend(disk_total)
        all_disk_sda1_data.extend(disk_sda1)
        all_disk_sda15_data.extend(disk_sda15)

        cpu_stats = compute_stats(cpu_data)
        memory_stats = compute_stats(memory_data)
        disk_total_stats = compute_stats(disk_total)
        disk_sda1_stats = compute_stats(disk_sda1)
        disk_sda15_stats = compute_stats(disk_sda15)

        def format_stats(stats):
            if stats is None:
                return ["N/A", "N/A", "N/A", "N/A"]
            return [f"{stats[0]:.1f}", f"{stats[1]:.1f}", f"{stats[2]:.1f}", f"{stats[3]:.1f}"]

        table_data = [
            ["CPU", *format_stats(cpu_stats)],
            ["RAM", *format_stats(memory_stats)],
            ["Disk", *format_stats(disk_total_stats)],
            ["sda1", *format_stats(disk_sda1_stats)],
            ["sda15", *format_stats(disk_sda15_stats)],
        ]

        print(f"\n===== Metrics Summary for {vm_name} =====")
        print(tabulate(table_data, headers=["Metrics", "Min", "Max", "Median", "Mean"], tablefmt="pretty"))

    # Compute combined stats
    combined_cpu_stats = compute_stats(all_cpu_data)
    combined_memory_stats = compute_stats(all_memory_data)
    combined_disk_total_stats = compute_stats(all_disk_total_data)
    combined_disk_sda1_stats = compute_stats(all_disk_sda1_data)
    combined_disk_sda15_stats = compute_stats(all_disk_sda15_data)

    def format_stats(stats):
        if stats is None:
            return ["N/A", "N/A", "N/A", "N/A"]
        return [f"{stats[0]:.1f}", f"{stats[1]:.1f}", f"{stats[2]:.1f}", f"{stats[3]:.1f}"]

    combined_table = [
        ["CPU", *format_stats(combined_cpu_stats)],
        ["RAM", *format_stats(combined_memory_stats)],
        ["Disk", *format_stats(combined_disk_total_stats)],
        ["sda1", *format_stats(combined_disk_sda1_stats)],
        ["sda15", *format_stats(combined_disk_sda15_stats)],
    ]

    print(f"\n===== Combined Stats (All VMs) =====")
    print(tabulate(combined_table, headers=["Metrics", "Min", "Max", "Median", "Mean"], tablefmt="pretty"))

if __name__ == "__main__":
    main()
