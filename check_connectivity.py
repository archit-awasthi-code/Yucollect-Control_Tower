import socket
import sys

# Database hosts to check
hosts = [
    {
        "name": "PostgreSQL Server",
        "host": "qa-postgres-rds.yucollect.in",
        "port": 5432
    },
    {
        "name": "MongoDB Server",
        "host": "nonprd-yucollect-cluster-pl-0.ax1ah.mongodb.net",
        "port": 27017
    }
]

for host_info in hosts:
    print(f"\nChecking connectivity to {host_info['name']}...")
    
    # Try to resolve hostname
    try:
        print(f"Resolving hostname {host_info['host']}...")
        host_ip = socket.gethostbyname(host_info['host'])
        print(f"✓ Hostname resolves to IP: {host_ip}")
        
        # Try to connect to the port
        try:
            print(f"Attempting to connect to {host_info['host']}:{host_info['port']}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host_info['host'], host_info['port']))
            s.close()
            print(f"✓ Successfully connected to {host_info['host']}:{host_info['port']}")
        except Exception as e:
            print(f"✗ Could not connect to {host_info['host']}:{host_info['port']}")
            print(f"  Error: {str(e)}")
            
    except Exception as e:
        print(f"✗ Could not resolve hostname {host_info['host']}")
        print(f"  Error: {str(e)}")

print("\nSummary:")
print("If both hostname resolution and connection tests failed, you likely need to:")
print("1. Connect to the required VPN to access these database servers, or")
print("2. Check if the database servers are still operational at these addresses")
