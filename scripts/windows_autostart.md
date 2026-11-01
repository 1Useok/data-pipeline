# Windows 24/7 Autostart Checklist

The data path uses the existing Windows mapping
`Z:` -> `\\fileserver\market_data`. Docker does not inherit that mapping, so
the setup script mounts it into the `docker-desktop` WSL VM before starting
the data services. No SMB password is stored in the repository or Docker.

1. Enable Docker Desktop startup on login.
2. Disable sleep and hibernate for the workstation.
3. Prepare the SMB volume and register the five-minute recovery watchdog once:

```powershell
cd C:\path\to\data-pipeline
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_smb_data_volume.ps1 -RegisterWatchdog
```

The same command without `-RegisterWatchdog` can be run at any time to repair
the mount and start the stack.

4. Verify the stack:

```powershell
cd C:\path\to\data-pipeline\docker
docker compose ps
docker compose exec airflow-scheduler airflow config get-value core default_timezone
docker compose logs airflow-init
```

Expected services are `postgres`, `airflow-webserver`, `airflow-scheduler`, and
`dashboard`. Airflow should report `Asia/Seoul`, and `airflow-init` should show
the `catch_up` boot trigger attempt.
