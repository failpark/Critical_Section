alias run := up
runtime := "podman"
compose := if runtime == "podman" { "podman compose" } else { "docker-compose" }
export PODMAN_COMPOSE_PROVIDER := "podman-compose"

_default:
	@just --list

demo:
	uv run scripts/demo.py

ps:
	{{runtime}} ps -a

[group('local')]
build:
	{{runtime}} build -t critical-section .

[group('local')]
up:
	{{compose}} up --build --force-recreate --remove-orphans

[group('local')]
down:
	{{compose}} down

[group('local')]
watch-logs:
	{{compose}} logs -f

[group('local')]
kill-prime:
	{{compose}} stop primary

[group('local')]
nuclear:
	podman rm -f $(podman ps -aq)

# Example VLAN deployment (3 laptops: 192.168.1.101, .102, .103):
#   Laptop .101: just vlan-prime 192.168.1.102 192.168.1.103
#   Laptop .102: just vlan-backup 99 192.168.1.101 192.168.1.103
#   Laptop .103: just vlan-backup 98 192.168.1.101 192.168.1.102
#   Any laptop:  just vlan-node 192.168.1.101

[group('vlan')]
vlan-prime backup1-ip backup2-ip:
	{{runtime}} run --network=host critical-section uv run critical/coordinator.py --role primary --id 100 --peers {{backup1-ip}}:50001,{{backup2-ip}}:50001

[group('vlan')]
vlan-backup id prime-ip other-backup-ip:
	{{runtime}} run --network=host critical-section uv run critical/coordinator.py --role backup --id {{id}} --peers {{prime-ip}}:50001,{{other-backup-ip}}:50001

[group('vlan')]
vlan-node prime-ip:
	{{runtime}} run --network=host -e COORD_IP={{prime-ip}} critical-section

