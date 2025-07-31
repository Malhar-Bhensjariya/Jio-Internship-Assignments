import os
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.resource import SubscriptionClient
from tabulate import tabulate
import pytz

LOCATION = 'westindia'
LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')

credential = DefaultAzureCredential()
subscription_client = SubscriptionClient(credential)

subscription = next(subscription_client.subscriptions.list())
subscription_id = subscription.subscription_id

compute_client = ComputeManagementClient(credential, subscription_id)
monitor_client = MonitorManagementClient(credential, subscription_id)

def get_vms_by_tag(tag_key, tag_value):
    vms = []
    for vm in compute_client.virtual_machines.list_all():
        vms.append({
            'name':vm.name,
            'id':vm.id,
            'location':vm.location            
        })
    return vms

def fetch_metric(vm, metric_name, strat_time, end_time, unit_conversion=None):
    timespan = f"{strat_time}/{end_time}"
    metrics_data = monitor_client.metrics.list(
        vm['id'],
        timespan=timespan,
        interval='PT1M',
        metricnames=metric_name,
        aggregation='Average'
    )
    
    values = []
    for item in metrics_data.value:
        for timeseries in item.timeseries:
            for data in timeseries.data:
                if data.average is not None:
                    val = data.average
                    if unit_conversion:
                        val = unit_conversion(val)
                    values.append(val)
    
    return values

def compute_stats(data):
    if not data:
        return None
    
    data = sorted(data)
    n = len(data)
    median = data[n // 2] if n % 2 else (data[n // 2 - 1] + data[n // 2]) / 2
    mean = sum(data) / n
    return min(data), max(data), median, mean, n

def get_user_time_range():
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
        start_time_obj = datetime.min.time()
        end_time_obj = datetime.max.time()

    start_local = LOCAL_TIMEZONE.localize(datetime.combine(start_date, start_time_obj))
    end_local = LOCAL_TIMEZONE.localize(datetime.combine(end_date, end_time_obj))
    return start_local.isoformat(), end_local.isoformat()

def main():
    tag_key = input("Enter tag key (e.g., app): ").strip()
    tag_value = input("Enter tag value (e.g., lbtcp): ").strip()
    
    vms = get_vms_by_tag(tag_key, tag_value)
    
    if not vms:
        print(f"No VMs found with tag {tag_key}={tag_value}")
        return
    vm_names = [vm['name'] for vm in vms]
    print(f"Found VMs: {', '.join(vm_names)}")

    start_time, end_time = get_user_time_range()
    print(f"Fetching metrics from {start_time} to {end_time} (local time)")

    all_cpu_data = []
    all_memory_data = []
    all_disk_data = []

    for vm in vms:
        print(f"\nProcessing {vm['name']}...")

        cpu_data = fetch_metric(vm, 'Percentage CPU', start_time, end_time)
        all_cpu_data.extend(cpu_data)

        memory_data = fetch_metric(
            vm,
            'Available Memory Bytes',
            start_time,
            end_time,
            unit_conversion=lambda val: 100 - ((val / (8 * 1024 * 1024 * 1024)) * 100)
        )
        all_memory_data.extend(memory_data)

        disk_data = fetch_metric(vm, 'Used Disk Percentage', start_time, end_time)
        all_disk_data.extend(disk_data)

        cpu_stats = compute_stats(cpu_data)
        memory_stats = compute_stats(memory_data)
        disk_stats = compute_stats(disk_data)

        def format_stats(stats):
            if stats is None:
                return ["N/A", "N/A", "N/A", "N/A", "0"]
            return [f"{stats[0]:.1f}", f"{stats[1]:.1f}", f"{stats[2]:.1f}", f"{stats[3]:.1f}", f"{stats[4]}"]

        table_data = [
            ["CPU (%)", *format_stats(cpu_stats)],
            ["Memory (%)", *format_stats(memory_stats)],
            ["Disk (%)", *format_stats(disk_stats)],
        ]

        print(f"\n===== Metrics Summary for {vm['name']} =====")
        print(tabulate(table_data, headers=["Metric", "Min", "Max", "Median", "Mean", "Samples"], tablefmt="pretty"))

    if all_cpu_data or all_memory_data or all_disk_data:
        print("\n===== Combined Stats (All VMs) =====")

        combined_cpu_stats = compute_stats(all_cpu_data)
        combined_memory_stats = compute_stats(all_memory_data)
        combined_disk_stats = compute_stats(all_disk_data)

        combined_table = [
            ["CPU (%)", *format_stats(combined_cpu_stats)],
            ["Memory (%)", *format_stats(combined_memory_stats)],
            ["Disk (%)", *format_stats(combined_disk_stats)],
        ]

        print(tabulate(combined_table, headers=["Metric", "Min", "Max", "Median", "Mean", "Samples"], tablefmt="pretty"))

if __name__ == "__main__":
    main()