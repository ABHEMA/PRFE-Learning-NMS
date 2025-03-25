#!/usr/bin/env python3
import telnetlib
import time
import random

ROUTER_TELNET_HOST = "127.0.0.1"
ROUTER_TELNET_PORT = 5000
INTERFACES = ["Fa1/0", "Fa2/0"]

def link_down_up(interface):
    """
    Se connecte en Telnet au routeur, désactive une interface,
    attend un certain temps puis la réactive.
    """
    print(f"Connexion au routeur via Telnet {ROUTER_TELNET_HOST}:{ROUTER_TELNET_PORT}...")
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
    
    print(f"Interface {interface} désactivée")
    down_time = random.randint(10, 20)
    print(f"Interface désactivée pendant {down_time} secondes...")
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
    
    print("Simulation LinkDown/Up terminée.")

def main():
    interface = random.choice(INTERFACES)
    link_down_up(interface)
    while True:
        interface = random.choice(INTERFACES)
        sleep_time = random.randint(200, 300)
        print(f"Nouvelle opération dans {sleep_time} secondes...")
        time.sleep(sleep_time)
        try:
            link_down_up(interface)
        except Exception as e:
            print(f"Erreur lors de l'exécution de link_down_up : {e}")

if __name__ == "__main__":
    main()

