#!/usr/bin/env python3
import time
import csv
import paramiko
from pysnmp.hlapi import *

previous_snmp_values = {}

def snmp_poll(target, community, oid):
    errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(
            SnmpEngine(),
            CommunityData(community, mpModel=1),  # SNMPv2c
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

def get_interface_counters_snmp(ip, community, if_index):
    try:
        in_oid = f"1.3.6.1.2.1.31.1.1.1.6.{if_index}"
        out_oid = f"1.3.6.1.2.1.31.1.1.1.10.{if_index}"
        rx_bytes = int(snmp_poll(ip, community, in_oid))
        tx_bytes = int(snmp_poll(ip, community, out_oid))
        return rx_bytes, tx_bytes
    except Exception as e:
        print(f"SNMP error on {ip}: {e}")
        return None, None

def safe_float(val_str, default=0.0):
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
            "snmp_if_index": 3,
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
            "snmp_if_index": 3,
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
    fieldnames = ["timestamp", "source", "record_type", "metric", "value", "label",
                  "message"]

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
                        if float_val > 80:
                            label = "CPU_OVERLOAD"
                            message = f"{metric} usage > 80% (value={float_val})"

                    if device["type"] == "linux":
                        if metric == "cpuIdle":
                            cpu_usage = 100 - safe_float(raw_value_str, 0.0)
                            if cpu_usage > 80:
                                label = "CPU_OVERLOAD"
                                message = "CPU usage > 80%"
                            writer.writerow({
                                "timestamp": timestamp,
                                "source": device_name,
                                "record_type": record_type,
                                "metric": "cpuUsage",
                                "value": cpu_usage,
                                "label": label,
                                "message": message
                            })
                            csvfile.flush()
                        if metric == "cpuUser":
                            cpu_usage = safe_float(raw_value_str, 0.0)
                            if cpu_usage > 80:
                                label = "CPU_OVERLOAD"
                                message = "CPU usage > 80%"
                            writer.writerow({
                                "timestamp": timestamp,
                                "source": device_name,
                                "record_type": record_type,
                                "metric": "cpuUsage",
                                "value": cpu_usage,
                                "label": label,
                                "message": message
                            })
                            csvfile.flush()
                        if metric == "memUsed":
                            memTotal_val_str = snmp_poll(ip, community,
                                                         device["oids"].get("memTotal"))
                            memTotal_float = safe_float(memTotal_val_str, 0.0)
                            if memTotal_float > 0:
                                mem_usage = (float_val / memTotal_float) * 100
                                if mem_usage > 90:
                                    label = "MEM_OVERLOAD"
                                    message = f"Memory usage > 90% ({mem_usage:.1f}%)"
                                writer.writerow({
                                    "timestamp": timestamp,
                                    "source": device_name,
                                    "record_type": record_type,
                                    "metric": "memUsage",
                                    "value": mem_usage,
                                    "label": label,
                                    "message": message
                                })
                                csvfile.flush()

                    elif device["type"] == "cisco" and metric == "cpu5sec":
                        if float_val > 80:
                            label = "CPU_OVERLOAD"
                            message = f"CPU > 80% (5sec avg: {float_val})"

                    writer.writerow({
                        "timestamp": timestamp,
                        "source": device_name,
                        "record_type": record_type,
                        "metric": metric,
                        "value": str(float_val),
                        "label": label,
                        "message": message
                    })
                    csvfile.flush()

                if device["type"] == "linux" and "snmp_if_index" in device:
                    rx, tx = get_interface_counters_snmp(ip, community, device["snmp_if_index"])
                    if rx is not None and tx is not None:
                        rx_key = (device_name, "rx")
                        tx_key = (device_name, "tx")

                        rate_rx = (rx - 
                                    previous_snmp_values.get(rx_key, rx)) / polling_interval
                        rate_tx = (tx -
                                    previous_snmp_values.get(tx_key, tx)) / polling_interval

                        previous_snmp_values[rx_key] = rx
                        previous_snmp_values[tx_key] = tx

                        writer.writerow({
                            "timestamp": timestamp,
                            "source": device_name,
                            "record_type": record_type,
                            "metric": "rx_rate",
                            "value": rate_rx,
                            "label": "HIGH_RX" if rate_rx > 1e6 else "",
                            "message": ""
                        })
                        writer.writerow({
                            "timestamp": timestamp,
                            "source": device_name,
                            "record_type": record_type,
                            "metric": "tx_rate",
                            "value": rate_tx,
                            "label": "HIGH_TX" if rate_tx > 1e6 else "",
                            "message": ""
                        })
                        csvfile.flush()

            time.sleep(polling_interval)

if __name__ == "__main__":
    poll_metrics()

