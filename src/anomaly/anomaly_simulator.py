#!/usr/bin/env python3
import paramiko
import telnetlib
import time
import random

VM1 = {
    "host": "VM1_MGMT_IP",
    "user": "user",
    "password": "password",
    "vm_ip": "10.0.0.1",
    "other_vm_ip": "20.0.0.1"
}

VM2 = {
    "host": "VM2_MGMT_IP",
    "user": "user",
    "password": "password",
    "vm_ip": "20.0.0.1",
    "other_vm_ip": "10.0.0.1"
}

ROUTER_TELNET_HOST = "127.0.0.1"
ROUTER_TELNET_PORT = 5000
INTERFACES = ["Fa1/0", "Fa2/0"]

def ssh_command(host, user, password, cmd):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=5)
    stdin, stdout, stderr = client.exec_command(cmd)
    exit_code = stdout.channel.recv_exit_status()
    out_str = stdout.read().decode()
    err_str = stderr.read().decode()
    client.close()
    return (out_str, err_str, exit_code)

def link_down_up(interface):
    print(f"=== [ROUTER] LinkDown/Up on {interface} ===")
    try:
        tn = telnetlib.Telnet(ROUTER_TELNET_HOST, ROUTER_TELNET_PORT, timeout=5)
        tn.read_until(b"#", timeout=5)

        tn.write(b"conf t\r\n")
        tn.read_until(b"(config)#", timeout=5)

        tn.write(f"interface {interface}\r\n".encode('ascii'))
        tn.read_until(b"(config-if)#", timeout=5)
        tn.write(b"shutdown\r\n")
        time.sleep(0.5)
        tn.write(b"end\r\n")
        tn.read_until(b"#", timeout=5)

        tn.write(b"wr\r\n")
        tn.read_until(b"#", timeout=5)

        print(f"   -> Interface {interface} est désactivée.")
        down_time = random.randint(20, 40)
        print(f"   -> Attente {down_time} secondes avant réactivation.")
        time.sleep(down_time)

        tn.write(b"conf t\r\n")
        tn.read_until(b"(config)#", timeout=5)
        tn.write(f"interface {interface}\r\n".encode('ascii'))
        tn.read_until(b"(config-if)#", timeout=5)
        tn.write(b"no shutdown\r\n")
        time.sleep(0.5)
        tn.write(b"end\r\n")
        tn.read_until(b"#", timeout=5)

        tn.write(b"wr\r\n")
        tn.read_until(b"#", timeout=5)
        tn.close()

        print(f"   [OK] LinkDown/Up terminé sur {interface}.\n")
    except Exception as e:
        print(f"   [ERREUR Telnet] {e}\n")

def generate_vm_event():
    chosen_vm = random.choice([VM1, VM2])
    event_type = random.choice(["CPU", "MEM", "IPERF", "PING"])

    if event_type == "CPU":
        duration = random.randint(60, 300)
        cpu_count = random.randint(1, 2)
        return {
            "name": f"CPU_{chosen_vm['host']}",
            "vm": chosen_vm,
            "cmd": f"stress -c {cpu_count} --timeout {duration}"
        }

    elif event_type == "MEM":
        mem_size = random.choice([512, 1024, 2048])
        duration = random.randint(60, 300)
        return {
            "name": f"MEM_{chosen_vm['host']}",
            "vm": chosen_vm,
            "cmd": f"stress --vm 1 --vm-bytes {mem_size}M --timeout {duration}"
        }

    elif event_type == "IPERF":
        duration = random.randint(60, 300)
        return {
            "name": f"IPERF_{chosen_vm['host']}",
            "vm": chosen_vm,
            "cmd": f"iperf3 -c {chosen_vm['other_vm_ip']} -t {duration}"
        }

    else:
        count = random.randint(60, 120)
        return {
            "name": f"PING_{chosen_vm['host']}",
            "vm": chosen_vm,
            "cmd": f"ping -c {count} {chosen_vm['other_vm_ip']}"
        }

def simulate_vm_event(event):
    name = event["name"]
    vm   = event["vm"]
    cmd  = event["cmd"]

    print(f"\n=== [VM EVENT] {name} on {vm['host']} ===")
    print(f"    Command: {cmd}")
    try:
        out, err, code = ssh_command(vm["host"], vm["user"], vm["password"], cmd)
        if code == 0:
            print(f"    [OK] Command executed successfully.\n")
        else:
            print(f"    [ERREUR] Exit code={code}, stderr={err}\n")
    except Exception as e:
        print(f"    [EXCEPTION SSH] {e}\n")

def run_vm_events(num_events):
    for i in range(num_events):
        event = generate_vm_event()
        simulate_vm_event(event)

        pause = random.randint(30, 90)
        print(f"[INFO] Attente {pause} secondes avant l'événement VM suivant...\n")
        time.sleep(pause)

def main():
    print("\n============================")
    print(" DÉMARRAGE DE LA SIMULATION ")
    print("============================\n")

    cycle_count = 0

    while True:
        cycle_count += 1
        print(f"==== [CYCLE #{cycle_count}] ====")

        nb_before = random.randint(1, 3)
        print(f"[CYCLE] Lancement de {nb_before} événement(s) VM avant le LinkDown...")
        run_vm_events(nb_before)

        chosen_if = random.choice(INTERFACES)
        link_down_up(chosen_if)

        pause2 = random.randint(60, 120)
        print(f"[CYCLE] Attente {pause2} secondes avant la suite...\n")
        time.sleep(pause2)

        nb_after = random.randint(1, 3)
        print(f"[CYCLE] Lancement de {nb_after} événement(s) VM après le LinkDown...")
        run_vm_events(nb_after)

        pause3 = random.randint(180, 300)
        print(f"Fin du cycle #{cycle_count}, prochain cycle dans ~{pause3} secondes...\n")
        time.sleep(pause3)

if __name__ == "__main__":
    main()
