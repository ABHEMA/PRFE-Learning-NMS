#!/usr/bin/env python3
import time
import csv
import paramiko
from pysnmp.hlapi import *

previous_snmp_values = {}
previous_ssh_values = {}

def snmp_poll(target, community, oid):
    errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(
            SnmpEngine(),
            CommunityData(community, mpModel=1),
            UdpTransportTarget((target, 161), timeout=2, retries=1),
            ContextData(),
            ObjectType(ObjectIdentity(oid))
        )
    )
    if errorIndication or errorStatus:
        return None
    for varBind in varBinds:
        return varBind[1].prettyPrint()
    return None

def get_interface_counters_ssh(host, username, password, interface):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(host, username=username, password=password, timeout=5)
        stdin, stdout, stderr = client.exec_command("cat /proc/net/dev")
        output = stdout.read().decode()
        client.close()
        for line in output.splitlines():
            if line.strip().startswith(interface + ":"):
                parts = line.split(":")[1].strip().split()
                rx_bytes = int(parts[0])
                tx_bytes = int(parts[8])
                return rx_bytes, tx_bytes
    except Exception as e:
        print(f"SSH error on {host}: {e}")
    return None, None

def safe_float(val_str, default=0.0):
    """
    Attempt to parse val_str as float.
    If it fails or is empty, return default.
    """
    if not val_str:
        return default
    val_str = val_str.replace("Gauge32:", "").strip()
    try:
        return float(val_str)
    except ValueError:
        print(f"Could not convert '{val_str}' to float, defaulting to {default}")
        return default

def poll_metrics():
    devices = {
        "VM1": {
            "type": "linux",
            "ip": "1.0.0.2",
            "community": "public",
            "ssh_interface": "enp0s8",
            "ssh_user": "max",
            "ssh_pass": "max",
            "oids": {
                "sysUpTime": ".1.3.6.1.2.1.1.3.0",
                "load1": ".1.3.6.1.4.1.2021.10.1.3.1",
                "load5": ".1.3.6.1.4.1.2021.10.1.3.2",
                "load15": ".1.3.6.1.4.1.2021.10.1.3.3",
                "cpuUser": ".1.3.6.1.4.1.2021.11.9.0",
                "cpuSystem": ".1.3.6.1.4.1.2021.11.10.0",
                "cpuIdle": ".1.3.6.1.4.1.2021.11.11.0",
                "memTotal": ".1.3.6.1.4.1.2021.4.5.0",
                "memUsed": ".1.3.6.1.4.1.2021.4.6.0",
                "memFree": ".1.3.6.1.4.1.2021.4.11.0",
                "swapTotal": ".1.3.6.1.4.1.2021.4.3.0",
                "swapAvail": ".1.3.6.1.4.1.2021.4.4.0"
            }
        },
        "VM2": {
            "type": "linux",
            "ip": "1.0.0.3",
            "community": "public",
            "ssh_interface": "enp0s8",
            "ssh_user": "max",
            "ssh_pass": "max",
            "oids": {
                "sysUpTime": ".1.3.6.1.2.1.1.3.0",
                "load1": ".1.3.6.1.4.1.2021.10.1.3.1",
                "load5": ".1.3.6.1.4.1.2021.10.1.3.2",
                "load15": ".1.3.6.1.4.1.2021.10.1.3.3",
                "cpuUser": ".1.3.6.1.4.1.2021.11.9.0",
                "cpuSystem": ".1.3.6.1.4.1.2021.11.10.0",
                "cpuIdle": ".1.3.6.1.4.1.2021.11.11.0",
                "memTotal": ".1.3.6.1.4.1.2021.4.5.0",
                "memUsed": ".1.3.6.1.4.1.2021.4.6.0",
                "memFree": ".1.3.6.1.4.1.2021.4.11.0",
                "swapTotal": ".1.3.6.1.4.1.2021.4.3.0",
                "swapAvail": ".1.3.6.1.4.1.2021.4.4.0"
            }
        },
        "R1": {
            "type": "cisco",
            "ip": "1.0.0.4",
            "community": "public",
            "oids": {
                "sysUpTime": ".1.3.6.1.2.1.1.3.0",
                "load1": ".1.3.6.1.4.1.9.9.109.1.1.1.1.6.1",
                "load5": ".1.3.6.1.4.1.9.9.109.1.1.1.1.7.1",
                "load15": ".1.3.6.1.4.1.9.9.109.1.1.1.1.8.1",
                "memPoolUsed": ".1.3.6.1.4.1.9.9.48.1.1.1.5.1",
                "memPoolFree": ".1.3.6.1.4.1.9.9.48.1.1.1.6.1"
            }
        }
    }

    polling_interval = 5

    fieldnames = ["timestamp", "source", "record_type", "metric", "value", "label", "message"]

    with open("snmp_poll.csv", "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csvfile.seek(0, 2)
        if csvfile.tell() == 0:
            writer.writeheader()

        print("Starting SNMP polling (snmp_poll.csv)...")
        while True:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            for device_name, device in devices.items():
                record_type = "SNMP_POLL"
                ip = device["ip"]
                community = device["community"]

                for metric, oid in device.get("oids", {}).items():
                    label = ""
                    message = ""
                    raw_value_str = snmp_poll(ip, community, oid)

                    if raw_value_str is None:
                        row = {
                            "timestamp": timestamp,
                            "source": device_name,
                            "record_type": record_type,
                            "metric": metric,
                            "value": "",
                            "label": "",
                            "message": f"SNMP polling returned None for {oid}"
                        }
                        writer.writerow(row)
                        csvfile.flush()
                        continue

                    float_val = float(raw_value_str)

                    if metric in ["load1", "load5", "load15"]:
                        try:
                            if float_val > 80:
                                label = "CPU_OVERLOAD"
                                message = f"{metric} usage > 80% (value={float_val})"
                        except Exception as e:
                            print(f"Error converting {metric} on {device_name}: {e}")

                    if device["type"] == "linux":
                        if metric == "cpuIdle":
                            try:
                                cpu_usage = 100 - safe_float(raw_value_str, 0.0)
                                if cpu_usage > 80:
                                    label = "CPU_OVERLOAD"
                                    message = "CPU usage > 80%"
                                row_cpu = {
                                    "timestamp": timestamp,
                                    "source": device_name,
                                    "record_type": record_type,
                                    "metric": "cpuUsage",
                                    "value": cpu_usage,
                                    "label": label,
                                    "message": message
                                }
                                writer.writerow(row_cpu)
                                csvfile.flush()
                            except:
                                pass
                        if metric == "cpuUser":
                            try:
                                cpu_usage = safe_float(raw_value_str, 0.0)
                                if cpu_usage > 80:
                                    label = "CPU_OVERLOAD"
                                    message = "CPU usage > 80%"
                                row_cpu = {
                                    "timestamp": timestamp,
                                    "source": device_name,
                                    "record_type": record_type,
                                    "metric": "cpuUsage",
                                    "value": cpu_usage,
                                    "label": label,
                                    "message": message
                                }
                                writer.writerow(row_cpu)
                                csvfile.flush()
                            except:
                                pass
                        if metric == "memUsed":
                            try:
                                memTotal_val_str = snmp_poll(ip, community, device["oids"].get("memTotal"))
                                memTotal_float = safe_float(memTotal_val_str, 0.0)
                                if memTotal_float > 0:
                                    mem_usage = (float_val / memTotal_float) * 100
                                    if mem_usage > 90:
                                        label = "MEM_OVERLOAD"
                                        message = f"Memory usage > 90% ({mem_usage:.1f}%)"
                                    row_mem = {
                                        "timestamp": timestamp,
                                        "source": device_name,
                                        "record_type": record_type,
                                        "metric": "memUsage",
                                        "value": mem_usage,
                                        "label": label,
                                        "message": message
                                    }
                                    writer.writerow(row_mem)
                                    csvfile.flush()
                            except:
                                pass

                    elif device["type"] == "cisco":
                        if metric == "cpu5sec":
                            if float_val > 80:
                                label = "CPU_OVERLOAD"
                                message = f"CPU > 80% (5sec avg: {float_val})"

                    row = {
                        "timestamp": timestamp,
                        "source": device_name,
                        "record_type": record_type,
                        "metric": metric,
                        "value": str(float_val),
                        "label": label,
                        "message": message
                    }
                    writer.writerow(row)
                    csvfile.flush()

                if device["type"] == "linux":
                    rx, tx = get_interface_counters_ssh(ip, device["ssh_user"], device["ssh_pass"], device["ssh_interface"])
                    if rx is not None and tx is not None:
                        rx_key = (device_name, "rx")
                        tx_key = (device_name, "tx")

                        if rx_key in previous_ssh_values:
                            diff_rx = rx - previous_ssh_values[rx_key]
                            rate_rx = diff_rx / polling_interval
                        else:
                            rate_rx = 0
                        previous_ssh_values[rx_key] = rx

                        if tx_key in previous_ssh_values:
                            diff_tx = tx - previous_ssh_values[tx_key]
                            rate_tx = diff_tx / polling_interval
                        else:
                            rate_tx = 0
                        previous_ssh_values[tx_key] = tx

                        label_rx = "HIGH_RX" if rate_rx > 1e6 else ""
                        label_tx = "HIGH_TX" if rate_tx > 1e6 else ""

                        row_rx = {
                            "timestamp": timestamp,
                            "source": device_name,
                            "record_type": record_type,
                            "metric": "rx_rate",
                            "value": rate_rx,
                            "label": label_rx,
                            "message": ""
                        }
                        writer.writerow(row_rx)

                        row_tx = {
                            "timestamp": timestamp,
                            "source": device_name,
                            "record_type": record_type,
                            "metric": "tx_rate",
                            "value": rate_tx,
                            "label": label_tx,
                            "message": ""
                        }
                        writer.writerow(row_tx)
                        csvfile.flush()

            time.sleep(polling_interval)

if __name__ == "__main__":
    poll_metrics()

