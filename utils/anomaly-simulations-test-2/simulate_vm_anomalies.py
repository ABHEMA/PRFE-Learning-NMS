#!/usr/bin/env python3
import paramiko
import random
import time

VM1 = {
    "host": "192.168.1.20",
    "user": "max",
    "password": "max",
    "vm_ip": "10.0.0.1",
    "other_vm_ip": "20.0.0.1"
}
VM2 = {
    "host": "192.168.1.19",
    "user": "max",
    "password": "max",
    "vm_ip": "20.0.0.1",
    "other_vm_ip": "10.0.0.1"
}

def ssh_command(host, user, password, cmd):
    """
    SSH into a host and run 'cmd' (non-interactive).
    Returns (stdout_str, stderr_str, exit_code).
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=5)
    stdin, stdout, stderr = client.exec_command(cmd)
    exit_code = stdout.channel.recv_exit_status()
    out_str = stdout.read().decode()
    err_str = stderr.read().decode()
    client.close()
    return (out_str, err_str, exit_code)

def random_vm_anomaly():
    """
    Randomly picks an anomaly to generate on VM1 or VM2.
    Each command runs for about 60s.
    """
    possible = [
        ("CPU_VM1", VM1, "stress -c 1 --timeout 60"),
        ("CPU_VM2", VM2, "stress -c 1 --timeout 60"),
        ("MEM_VM1", VM1, "stress --vm 1 --vm-bytes 2048M --timeout 60"),
        ("MEM_VM2", VM2, "stress --vm 1 --vm-bytes 2048M --timeout 60"),
        ("IPERF_VM1_to_VM2", VM1, f"iperf3 -c {VM1['other_vm_ip']} -t 60"),
        ("IPERF_VM2_to_VM1", VM2, f"iperf3 -c {VM2['other_vm_ip']} -t 60"),
        ("IPERF_VM1_to_VM2", VM1, f"iperf3 -c {VM1['other_vm_ip']} -t 60"),
        ("IPERF_VM2_to_VM1", VM2, f"iperf3 -c {VM2['other_vm_ip']} -t 60"),
        ("PING_VM1_to_VM2", VM1, f"ping -c 60 {VM1['other_vm_ip']}"),
        ("PING_VM2_to_VM1", VM2, f"ping -c 60 {VM2['other_vm_ip']}")
    ]
    return random.choice(possible)

def main():
    print("Starting random VM anomaly generator...")
    while True:
        sleep_time = random.randint(60, 120)
        time.sleep(sleep_time)

        name, vm, cmd = random_vm_anomaly()
        print(f"\n=== Triggering anomaly: {name} on {vm['host']} with command: {cmd}")

        try:
            output = ssh_command(vm["host"], vm["user"], vm["password"], cmd)
            print(f"Output:\n{output}")
        except Exception as e:
            print(f"Error running anomaly on {vm['host']}: {e}")

if __name__ == "__main__":
    main()
