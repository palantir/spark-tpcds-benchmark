#!/bin/bash

set -euo pipefail

function die() {
  local MSG="${1}"
  echo "${MSG}"
  exit 1
}

function create_mount_path() {
    local MOUNT_PATH="${1}"
    mkdir $MOUNT_PATH
    chown root: $MOUNT_PATH
    chmod 1777 $MOUNT_PATH
}

function create_filesystem_and_mount() {
  local DEVICE="${1}"
  local MOUNT_PATH="${2}"
  [[ ! -d $MOUNT_PATH ]] && create_mount_path $MOUNT_PATH
  mkfs.xfs "${DEVICE}"
  mount -o "defaults,nosuid,noatime,nodev" "${DEVICE}" ${MOUNT_PATH}
  chmod 1777 "${MOUNT_PATH}"
}

function unmount_drive_if_mounted() {
  local DEVICE="${1}"
  set +e
  mount | grep "${DEVICE}" &>/dev/null
  [[ $? -ne 0 ]] && return
  set -e
  umount "${DEVICE}"
}

function luks_encrypt_drive() {
  local DEVICE="${1}"
  local KEY="${2}"
  local DEVICE_MAPPER_PATH="${3}"
  local DEVICE_MAPPER_BASENAME=$(basename $DEVICE_MAPPER_PATH)
  [[ -h ${DEVICE_MAPPER_PATH} ]] && cryptsetup -q luksClose "${DEVICE_MAPPER_PATH}"
  echo -n $KEY | cryptsetup -q luksFormat "${DEVICE}" -
  echo -n $KEY | cryptsetup -q luksOpen "${DEVICE}" "${DEVICE_MAPPER_BASENAME}" -
}

function ensure_drive_in_fstab() {
  local DEVICE_MAPPER_PATH="${1}"
  local EPHEMERAL_MOUNT_PATH="${2}"
}

function stop_raid() {
  local DEVICE="${1}"
  [[ -e "${DEVICE}" ]] && mdadm --stop "${DEVICE}"
  [[ -e "${DEVICE}_0" ]] && mdadm --stop "${DEVICE}_0"
}

function create_raid() {
  local MD_DEVICE="$1"
  shift 1
  local DISK_COUNT="$#"
  local DISKS="$@"
  udevadm settle
  udevadm control --stop-exec-queue
  mdadm --create $MD_DEVICE --level=0 --run --raid-devices=$DISK_COUNT $DISKS --verbose
  udevadm control --start-exec-queue
  udevadm settle
}

EPHEMERAL_MOUNT_PATH=/scratch
LUKS_KEY=$(dd status=none bs=1 count=1024 if=/dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -1)
DEVICE_MAPPER_PATH=/dev/mapper/ephemeral_drive
EPHEMERAL_DEVICE=$(readlink -f /dev/disk/azure/resource-part1)

[[ -z ${EPHEMERAL_DEVICE} ]] && die "Unable to locate ephemeral device!"

unmount_drive_if_mounted $EPHEMERAL_DEVICE
luks_encrypt_drive $EPHEMERAL_DEVICE $LUKS_KEY $DEVICE_MAPPER_PATH

create_filesystem_and_mount $DEVICE_MAPPER_PATH $EPHEMERAL_MOUNT_PATH
ensure_drive_in_fstab $DEVICE_MAPPER_PATH $EPHEMERAL_MOUNT_PATH

NVME_EPHEMERAL_DEVICES=$(ls /dev/disk/by-id/nvme-Microsoft_NVMe_Direct_Disk_*)

if [[ -z ${NVME_EPHEMERAL_DEVICES} ]]; then
  echo "No NVMe devices found."
else
  NVME_DEVICE_MD_PATH=/dev/md/ephemeral_drive
  NVME_DEVICE_MAPPER_PATH=/dev/mapper/nvme_ephemeral_drive
  NVME_EPHEMERAL_MOUNT_PATH=/nvme_scratch
fi

unmount_drive_if_mounted $NVME_DEVICE_MAPPER_PATH
stop_raid $NVME_DEVICE_MD_PATH
create_raid $NVME_DEVICE_MD_PATH $NVME_EPHEMERAL_DEVICES

RESOLVED_DEVICE_MD_PATH="$(readlink -f $NVME_DEVICE_MD_PATH)"
luks_encrypt_drive $RESOLVED_DEVICE_MD_PATH $LUKS_KEY $NVME_DEVICE_MAPPER_PATH

create_filesystem_and_mount $NVME_DEVICE_MAPPER_PATH $NVME_EPHEMERAL_MOUNT_PATH
ensure_drive_in_fstab $NVME_DEVICE_MAPPER_PATH $NVME_EPHEMERAL_MOUNT_PATH

echo "Done."
