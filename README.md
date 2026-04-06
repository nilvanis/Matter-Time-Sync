# Matter Time Sync

This repository is a fork of the excellent original component by Loweack:

https://github.com/Loweack/Matter-Time-Sync

It is a Home Assistant custom component for synchronizing time and timezone data to Matter devices that support the Time Synchronization cluster.

## What Changed In This Fork

- Reduced noisy Home Assistant logs when a device is offline or unavailable.
- Added a Matter-standard timezone/DST sync path first, with compatibility fallback for devices that need merged-offset behavior.
- Moved the manual sync button to `CONFIG` category.
