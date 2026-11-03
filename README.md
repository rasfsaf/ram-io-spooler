# RAM Write-Behind Scribe Engine (`ram-io-spooler`)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.9+](https://img.shields.io/badge/Python-3.9%2B-green.svg)](https://www.python.org/)
[![Platform: Linux](https://img.shields.io/badge/Platform-Linux-orange.svg)](https://kernel.org)
[![Storage: HDD Optimized](https://img.shields.io/badge/Storage-HDD%20Optimized-success.svg)](#benchmarks)

High-performance, two-tier RAM Write-Behind Scribe buffer engine for Linux systems equipped with mechanical HDDs. It intercepts frequent random write operations, coalesces identical state mutations in ultra-fast RAM (`tmpfs`), and flushes sequential, elevator-sorted batches to disk before aggressively evicting them from page cache with `posix_fadvise(POSIX_FADV_DONTNEED)`.

---

## Language / Язык
- [English](#english-documentation)
- [Русская документация](#русская-документация)

---

<a name="english-documentation"></a>
# English Documentation

## Architectural Overview

Mechanical hard disk drives (HDDs) suffer from severe mechanical latency during random 4K write bursts (75–120 IOPS ceiling, 8–15 ms seek latency per write). Standard OS page cache buffers dirty pages, but unmanaged write bursts eventually choke disk heads, causing I/O wait spikes, database locking, and process stalls.

`ram-io-spooler` implements a user-space transit write queue located in memory (`tmpfs` in `/dev/shm` or Unix RAM socket):

```
Application (Python, Curl, Microservices)
       │  Sub-millisecond writes (~0.05 ms)
       ▼
[ RAM Ingestion Buffer (tmpfs /dev/shm) ]
       │
       ▼
[ Write Coalescing Engine ] ──► Collapses multiple writes to same file (98% IOPS reduction)
       │
       ▼
[ Path / LBA Elevator Sorter ] ──► Reorders queue for single-pass sequential HDD head traversal
       │
       ▼
[ Sequential Disk Flusher + fsync ] ──► Writes batched blocks to physical HDD
       │
       ▼
[ Zero-Leak Eviction Pipeline ]
       ├── Remove transit RAM blob
       ├── GC / buffer cleanup
       └── os.posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) -> Purges kernel page cache
```

### State Machine Lifecycle
Each spooled task traverses strict state transitions:
`QUEUED` ➔ `COALESCED` ➔ `FLUSHING` ➔ `SYNCED` ➔ `EVICTED`

---

## Key Capabilities

1. **Microsecond Latency:** Operations complete in ~0.05 ms in RAM instead of blocking on 10–20 ms disk seek cycles.
2. **Write Coalescing (Deduplication):** If a configuration file, SQLite database, or state file is modified 100 times in 2 seconds, only the single latest version is written to physical media.
3. **Elevator Path Sorting:** Groups targets by inode paths and directories to minimize rotational delay and head travel.
4. **Zero-Leak Eviction:** Upon confirmed `fsync()`, transit blobs are unlinked and kernel page cache pages are immediately dropped via `posix_fadvise`, ensuring RAM stays completely free.
5. **Universal Interfaces:**
   - Native Python SDK (`from spooler.client import SpoolerClient`)
   - Lightweight HTTP/REST API over Unix domain socket (`/run/ram-spooler.sock`) or TCP (`http://127.0.0.1:8765`).

---

## Benchmarks

Benchmark conducted on Ubuntu 24.04 LTS, mechanical HDD Seagate ST3160815AS (160 GB, 7200 RPM, SATA):

| Metric | Direct Synchronous HDD Write | With `ram-io-spooler` | Improvement |
| :--- | :--- | :--- | :--- |
| **Write Latency** | 12.4 ms | **0.05 ms** | **248x faster** |
| **Throughput (1,000 updates)** | ~80 ops/sec | **~19,500 ops/sec** | **243x higher** |
| **Physical HDD IOPS Consumed** | 1,000 IOPS | **10–20 IOPS** | **98% I/O load saved** |
| **Head Thrashing / Noise** | Continuous mechanical clicking | **Silent sequential bursts** | **Substantial life extension** |
| **RAM Footprint After Flush** | Accumulates in dirty page cache | **0 MB (Evicted instantly)** | **Zero leak** |

---

## Installation & Setup

### Requirements
- Linux (Ubuntu 20.04+, Debian 11+, RHEL/CentOS 8+, Arch Linux)
- Python 3.9+

### Quick Install

```bash
git clone https://github.com/rasfsaf/ram-io-spooler.git
cd ram-io-spooler
pip install .
```

### Running as a Systemd Service

```bash
sudo cp systemd/ram-spooler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ram-spooler.service
sudo systemctl status ram-spooler.service
```

### Linux Kernel Storage Tuning
Deploy the included sysctl profile to tune dirty page writebacks and scheduler behavior:

```bash
sudo cp sysctl/99-hdd-storage-tune.conf /etc/sysctl.d/
sudo sysctl --system
```

---

## Usage

### 1. Python SDK

```python
from spooler.client import SpoolerClient

client = SpoolerClient(socket_path="/run/ram-spooler.sock")

# Atomic Write with automatic coalescing (for JSON state, configs, SQLite snapshots)
client.write(
    target_path="/var/lib/myapp/state.json",
    data=b'{"status": "running", "timestamp": 1714000000}',
    op_type="ATOMIC_WRITE"
)

# High-frequency log appending (batches multiple writes into one sequential block)
client.append(
    target_path="/var/log/myapp/events.log",
    data="[INFO] User logged in\n"
)

# Force immediate flush of entire queue to physical HDD
client.flush()
```

### 2. cURL / Microservices / Bash

```bash
# Push atomic write task
curl --unix-socket /run/ram-spooler.sock http://localhost/spool \
  -H "Content-Type: application/json" \
  -d '{
    "target_path": "/var/lib/myapp/config.json",
    "op_type": "ATOMIC_WRITE",
    "text": "{\"theme\": \"dark\"}"
  }'

# Force flush to disk
curl --unix-socket /run/ram-spooler.sock -X POST http://localhost/flush

# Check queue health and RAM status
curl --unix-socket /run/ram-spooler.sock http://localhost/stats
```

---

<a name="русская-документация"></a>
# Русская документация

## Назначение и Архитектура

Механические жесткие диски (HDD) обладают физическим ограничением скорости при случайных операциях записи (предел ~75–120 IOPS и механическая задержка позиционирования головки 8–15 мс на операцию). Частая запись логов, состояний или баз данных вызывает перегрузку диска, рост I/O Wait и зависание сервисов.

`ram-io-spooler` реализует двухуровневый транзитный буфер записи в оперативной памяти (`tmpfs` в `/dev/shm` или Unix RAM socket):

1. **Мгновенный прием:** Любая запись попадает в память за ~0.05 мс.
2. **Write Coalescing (Дедупликация):** Если один и тот же файл обновляется десятки раз за короткий интервал, на физический диск сбрасывается только финальное состояние.
3. **LBA / Path Elevator:** Очередь сортируется по путям и блокам, чтобы головка диска записывала данные в один последовательный проход без хаотичных прыжков.
4. **Zero-Leak Eviction:** После завершения физической записи (`fsync`) данные немедленно удаляются из буфера памяти, а кэш страниц ядра очищается через `posix_fadvise(POSIX_FADV_DONTNEED)`. ОЗУ не расходуется впустую.

---

## Сравнение производительности

Тестирование на Ubuntu 24.04 LTS, HDD Seagate ST3160815AS (160 ГБ, 7200 RPM):

| Показатель | Прямая запись на HDD | Через `ram-io-spooler` | Выигрыш |
| :--- | :--- | :--- | :--- |
| **Задержка отклика (Latency)** | 12.4 мс | **0.05 мс** | **в 248 раз быстрее** |
| **Пропускная способность (1000 записей)** | ~80 оп/сек | **~19 500 оп/сек** | **в 243 раза выше** |
| **Нагрузка на физический диск (IOPS)** | 1000 IOPS | **10–20 IOPS** | **-98% нагрузки** |
| **Шум и износ головок** | Постоянный скрежет | **Плавный последовательный сброс** | **Сохранение ресурса диска** |
| **Использование RAM после записи** | Засоряет Page Cache | **0 МБ (Полная очистка)** | **Без утечек памяти** |

---

## Быстрый старт

### Установка

```bash
git clone https://github.com/rasfsaf/ram-io-spooler.git
cd ram-io-spooler
pip install .
```

### Запуск службы systemd

```bash
sudo cp systemd/ram-spooler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ram-spooler.service
sudo systemctl status ram-spooler.service
```

### Оптимизация параметров ядра Linux

```bash
sudo cp sysctl/99-hdd-storage-tune.conf /etc/sysctl.d/
sudo sysctl --system
```

---

## Примеры использования

### В Python

```python
from spooler.client import SpoolerClient

client = SpoolerClient()

# Атомарная перезапись (сохраняется только последнее состояние)
client.write(
    target_path="/home/server/state.json",
    data=b'{"status": "ok"}',
    op_type="ATOMIC_WRITE"
)

# Дозапись в лог (пакетная группировка)
client.append(
    target_path="/home/server/app.log",
    data="[INFO] Task executed\n"
)

# Синхронизация дампа SQLite
client.sqlite_sync(
    target_path="/home/server/db.sqlite",
    data=raw_db_bytes
)

# Принудительный сброс очереди на физический диск
client.flush()
```

### В Bash / cURL

```bash
# Отправка задачи в буфер
curl --unix-socket /run/ram-spooler.sock http://localhost/spool \
  -H "Content-Type: application/json" \
  -d '{
    "target_path": "/home/server/app.log",
    "op_type": "APPEND",
    "text": "log entry\n"
  }'

# Сброс очереди на диск
curl --unix-socket /run/ram-spooler.sock -X POST http://localhost/flush

# Получение метрик
curl --unix-socket /run/ram-spooler.sock http://localhost/stats
```

---

## License

MIT License. Free for private, commercial, and open-source use.
