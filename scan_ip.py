import socket

def scan_port(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    try:
        i = sock.connect_ex((ip, port))
        return i
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        print ip, port, str(e)
    finally:
        sock.close()

for i in range(0, 256):
    ip = '192.168.101.%s' % i
    port = 80
    success = scan_port(ip, port)
    if success == 0:
        print ip, port, 'success'
