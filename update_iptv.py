#!/usr/bin/env python3
"""FreeIPTV: 从 iptv-org 下载各国 m3u 源，校验 m3u8 有效性后汇总到本地 index.m3u。

特性:
  - 读取 config.json 中的国家列表，下载对应 m3u 并解析频道信息
  - 使用 curl 校验播放地址: HTTP 200 且响应体包含 #EXTM3U 才视为有效
  - 仅将有效频道(含全部原始字段)写入本地 index.m3u
  - 支持脚本内置定时循环(--schedule)，也支持 cron 单次运行

依赖: Python 3.7+ (仅标准库) 与系统 curl 命令。
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATUS_MARKER = "##__HTTP_STATUS__##"


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_m3u(text):
    """解析 m3u 文本，返回频道条目列表。

    每个条目为 (header, extra_lines, url):
      header:     #EXTINF 行(含 tvg-id/tvg-name/tvg-logo/group-title 等全部字段)
      extra_lines: header 与 url 之间的其它注释行(如 #EXTVLCOPT:)
      url:        播放地址
    """
    entries = []
    lines = text.splitlines()
    header = None
    extra = []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line.startswith("#EXTINF"):
            header = line
            extra = []
        elif line.startswith("#") and header is not None:
            extra.append(line)
        elif line.strip() and not line.startswith("#"):
            if header is not None:
                entries.append((header, extra, line.strip()))
                header = None
                extra = []
    return entries


def check_url(url, timeout, require_extm3u, follow_redirect):
    """用 curl 校验地址: 返回 (valid: bool, status: str)。"""
    cmd = ["curl", "-s", "--max-time", str(timeout),
           "-w", STATUS_MARKER + "%{http_code}", url]
    if follow_redirect:
        cmd.insert(1, "-L")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"
    out = proc.stdout
    if STATUS_MARKER in out:
        body, status = out.rsplit(STATUS_MARKER, 1)
    else:
        body, status = out, ""
    status = status.strip()
    if status != "200":
        return False, status
    if require_extm3u and "#EXTM3U" not in body:
        return False, status + "(no-extm3u)"
    return True, status


def run_update(config, override_countries=None, override_output=None):
    base_url = config["base_url"].rstrip("/")
    output = override_output or config.get("output", "output/index.m3u")
    countries = override_countries or config.get("countries", [])
    check = config.get("check", {})
    timeout = check.get("timeout", 10)
    require_extm3u = check.get("require_extm3u", True)
    max_workers = check.get("max_workers", 20)
    follow_redirect = check.get("follow_redirect", True)
    dedup = config.get("dedup", True)

    log(f"开始更新，国家数={len(countries)}，并发={max_workers}，"
        f"校验规则=200{'+#EXTM3U' if require_extm3u else ''}")

    # 收集所有待校验条目
    pending = []  # (country, header, extra, url)
    for cc in countries:
        url = f"{base_url}/{cc}.m3u"
        try:
            proc = subprocess.run(["curl", "-s", "-L", "--max-time", "30", url],
                                  capture_output=True, text=True, timeout=35)
            if proc.returncode != 0 or not proc.stdout.strip():
                log(f"下载失败: {url}")
                continue
            entries = parse_m3u(proc.stdout)
            log(f"[{cc}] 解析到 {len(entries)} 个频道")
            for header, extra, play_url in entries:
                pending.append((cc, header, extra, play_url))
        except subprocess.TimeoutExpired:
            log(f"下载超时: {url}")
            continue

    log(f"共 {len(pending)} 个地址待校验")

    valid_entries = []
    seen = set()
    lock = threading.Lock()
    total = len(pending)
    done = 0
    ok = 0

    def worker(item):
        cc, header, extra, play_url = item
        valid, status = check_url(play_url, timeout, require_extm3u, follow_redirect)
        return cc, header, extra, play_url, valid, status

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(worker, it) for it in pending]
        for fut in as_completed(futures):
            cc, header, extra, play_url, valid, status = fut.result()
            with lock:
                done += 1
                if valid:
                    ok += 1
                    if dedup:
                        if play_url in seen:
                            valid = False
                        else:
                            seen.add(play_url)
                    if valid:
                        valid_entries.append((header, extra, play_url))
                if done % 50 == 0 or done == total:
                    log(f"进度 {done}/{total}  有效 {ok}")

    # 写出本地 index.m3u
    out_path = os.path.join(SCRIPT_DIR, output)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        f.write(f"# Generated by freeiptv at {datetime.now().isoformat()}\n")
        f.write(f"# Total valid channels: {len(valid_entries)}\n")
        for header, extra, play_url in valid_entries:
            f.write(header + "\n")
            for ex in extra:
                f.write(ex + "\n")
            f.write(play_url + "\n")

    log(f"更新完成: 有效频道 {len(valid_entries)}/{total} -> {output}")
    return len(valid_entries), total


def schedule_loop(config):
    interval_hours = config.get("schedule", {}).get("interval_hours", 24)
    interval = interval_hours * 3600
    log(f"进入定时模式，每 {interval_hours} 小时运行一次 (Ctrl+C 退出)")
    while True:
        try:
            run_update(config)
        except Exception as e:
            log(f"本次运行出错: {e}")
        log(f"下次运行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 起 {interval_hours} 小时后")
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="FreeIPTV 频道源更新工具")
    parser.add_argument("--config", default=os.path.join(SCRIPT_DIR, "config.json"),
                        help="配置文件路径 (默认 ./config.json)")
    parser.add_argument("--countries", nargs="*",
                        help="覆盖配置文件中的国家列表，例如 --countries cn us jp")
    parser.add_argument("--output", help="覆盖输出文件路径")
    parser.add_argument("--schedule", action="store_true",
                        help="以脚本内置定时循环方式运行 (受 config.schedule 控制间隔)")
    parser.add_argument("--once", action="store_true",
                        help="单次运行一次后退出 (默认行为)")
    args = parser.parse_args()

    config = load_config(args.config)

    if args.schedule or config.get("schedule", {}).get("enabled", False):
        schedule_loop(config)
    else:
        run_update(config,
                   override_countries=args.countries,
                   override_output=args.output)


if __name__ == "__main__":
    main()
