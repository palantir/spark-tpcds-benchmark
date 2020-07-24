#!/bin/bash
set -euexport NVME_DEVICE_MD_PATH=/dev/md/ephemeral_drive
export NVME_DEVICE_MAPPER_PATH=/dev/mapper/nvme_ephemeral_drive
export NVME_EPHEMERAL_MOUNT_PATH=/scratch
export NVME_EPHEMERAL_DEVICES=$(ls /dev/disk/by-id/nvme-Amazon_EC2_NVMe_Instance_Storage_*)mkdir $NVME_EPHEMERAL_MOUNT_PATH
chown root: $NVME_EPHEMERAL_MOUNT_PATH
chmod 1777 $NVME_EPHEMERAL_MOUNT_PATHmdadm --create $NVME_DEVICE_MD_PATH --level=0 --run --raid-devices=2 $NVME_EPHEMERAL_DEVICES --verbose
udevadm control --start-exec-queue
udevadm settle
mkfs.xfs $NVME_DEVICE_MD_PATH
mount -o "defaults,nosuid,noatime,nodev" $NVME_DEVICE_MD_PATH $NVME_EPHEMERAL_MOUNT_PATHchmod 1777 $NVME_EPHEMERAL_MOUNT_PATH
