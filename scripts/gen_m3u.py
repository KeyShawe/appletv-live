#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每天自动：扫描候选直播源 → 实测可用性与延迟 → 生成精简 tv-mini.m3u

保留范围：央视 18 台（CCTV-1~17 + CCTV-5+）+ 广东 8 台
输出规则：每台 1 个最快的主源 + 最多 2 个备用源（名称带 ·备）

用法：
    python scripts/gen_m3u.py                 # 输出到仓库根目录 tv-mini.m3u
    python scripts/gen_m3u.py /tmp/test.m3u   # 自定义输出路径
"""

import concurrent.futures
import datetime
import os
import re
import ssl
import sys
import time
import urllib.request

TIMEOUT = 9          # 单个源探测超时（秒）
WORKERS = 28         # 并发数
BACKUPS = 2          # 每个频道保留的备用源数量

UA = "Mozilla/5.0 (Apple TV; U; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15"
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE


# ---------------------------------------------------------------- 1. 频道定义
# (显示名, 分组, [URL 里必须出现的频道代码], 匹配公开列表时的正则)
CHANNELS = [
    ("CCTV-1 综合",      "央视", ["cctv1hd", "cctv1"],      r"cctv[-\s]?0?1\b|央视[-\s]?1"),
    ("CCTV-2 财经",      "央视", ["cctv2hd", "cctv2"],      r"cctv[-\s]?0?2\b|央视[-\s]?2"),
    ("CCTV-3 综艺",      "央视", ["cctv3hd", "cctv3"],      r"cctv[-\s]?0?3\b|央视[-\s]?3"),
    ("CCTV-4 中文国际",  "央视", ["cctv4hd", "cctv4"],      r"cctv[-\s]?0?4\b"),
    ("CCTV-5 体育",      "央视", ["cctv5hd", "cctv5"],      r"cctv[-\s]?0?5\b(?!\+)"),
    ("CCTV-5+ 体育赛事", "央视", ["cctv5p", "cctv5plus"],   r"cctv[-\s]?5\s?\+|cctv5p"),
    ("CCTV-6 电影",      "央视", ["cctv6hd", "cctv6"],      r"cctv[-\s]?0?6\b"),
    ("CCTV-7 国防军事",  "央视", ["cctv7hd", "cctv7"],      r"cctv[-\s]?0?7\b"),
    ("CCTV-8 电视剧",    "央视", ["cctv8hd", "cctv8"],      r"cctv[-\s]?0?8\b"),
    ("CCTV-9 纪录",      "央视", ["cctv9hd", "cctv9"],      r"cctv[-\s]?0?9\b"),
    ("CCTV-10 科教",     "央视", ["cctv10hd", "cctv10"],    r"cctv[-\s]?10\b"),
    ("CCTV-11 戏曲",     "央视", ["cctv11hd", "cctv11"],    r"cctv[-\s]?11\b"),
    ("CCTV-12 社会与法", "央视", ["cctv12hd", "cctv12"],    r"cctv[-\s]?12\b"),
    ("CCTV-13 新闻",     "央视", ["cctv13hd", "cctv13"],    r"cctv[-\s]?13\b"),
    ("CCTV-14 少儿",     "央视", ["cctv14hd", "cctv14"],    r"cctv[-\s]?14\b"),
    ("CCTV-15 音乐",     "央视", ["cctv15hd", "cctv15"],    r"cctv[-\s]?15\b"),
    ("CCTV-16 奥林匹克", "央视", ["cctv16hd", "cctv16"],    r"cctv[-\s]?16\b"),
    ("CCTV-17 农业农村", "央视", ["cctv17hd", "cctv17"],    r"cctv[-\s]?17\b"),
    ("广东卫视",  "广东", ["gdwshd", "gdws"],        r"广东卫视|广东卫星"),
    ("广东珠江",  "广东", ["gdzja", "gdzj"],         r"广东珠江|珠江频道"),
    ("广东体育",  "广东", ["gdty"],                  r"广东体育"),
    ("广东新闻",  "广东", ["gdxw"],                  r"广东新闻"),
    ("广东民生",  "广东", ["gdgg", "gdms"],          r"广东民生|广东公共"),
    ("深圳卫视",  "广东", ["szwshd", "szws"],        r"深圳卫视"),
    ("广州综合",  "广东", ["zonghes", "gzzonghe"],   r"广州综合"),
    ("广州新闻",  "广东", ["xinwen", "gzxinwen"],    r"广州新闻"),
]

# 央视走的主机（{} 处填频道代码，如 cctv1hd）
CCTV_HOSTS = [
    "http://74.91.26.218:82/live/{}.m3u8",
    "http://107.150.60.122/live/{}.m3u8",
    "http://63.141.230.178:82/gslb/zbdq5.m3u8?id={}",
    "http://38.75.136.137:98/gslb/dsdqbv/{}.m3u8?auth=test20251009",
    "http://198.204.228.26/live/{}.m3u8",
    "http://192.151.150.154/live/{}.m3u8",
]

# 广东/卫视走的主机
GD_HOSTS = [
    "http://38.75.136.137:98/gslb/dsdqpub/{}.m3u8?auth=testpub",
    "http://107.150.60.122/live/{}.m3u8",
    "http://63.141.230.178:82/gslb/zbdq5.m3u8?id={}",
    "http://74.91.26.218:82/live/{}.m3u8",
]

# 已验证的国内 tsfile 节点（key=txiptv），延迟通常 40~320ms，按频道号对应
TSFILE_HOST = "http://124.228.160.112:9901/tsfile/live/{}_1.m3u8?key=txiptv&playlive=1&authid=0"
TSFILE_IDS = {
    "CCTV-6 电影": "0006", "CCTV-7 国防军事": "0007", "CCTV-9 纪录": "0009",
    "CCTV-10 科教": "0010", "CCTV-15 音乐": "0015",
    "广东卫视": "0125", "深圳卫视": "0126",
}

# 每天去这几个公开仓库抓最新列表，发现新源（尤其是广州台带签名的链接）
DISCOVER_URLS = [
    "https://cdn.jsdelivr.net/gh/cs3306/IPTV-Sources@main/data/output/iptv_collection.m3u",
    "https://cdn.jsdelivr.net/gh/hujingguang/ChinaIPTV@master/cnTV1_ALL.m3u8",
    "https://cdn.jsdelivr.net/gh/best-fan/iptv-sources@main/cn_cctv.m3u8",
    "https://cdn.jsdelivr.net/gh/best-fan/iptv-sources@main/cn_province.m3u8",
]
# 这些域名虽然 URL 里没有频道代码，但名字对得上就收（官方/半官方 CDN）
TRUSTED_HOSTS = ("tencentplay.gztv.com", "liveplay-srs.voc.com.cn")


# ---------------------------------------------------------------- 2. 工具函数
def http_get(url, timeout=TIMEOUT, limit=4096):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return r.status, r.read(limit)


def probe(url):
    """返回 (延迟毫秒, 是否可用)"""
    t0 = time.time()
    try:
        status, body = http_get(url)
        ok = status == 200 and b"#EXT" in body
        return url, int((time.time() - t0) * 1000), ok
    except Exception:
        return url, 9999, False


def build_static_candidates():
    """主机 × 频道代码 = 静态候选矩阵"""
    cand = {}
    for name, grp, codes, _rx in CHANNELS:
        urls = []
        hosts = CCTV_HOSTS if name.startswith("CCTV") else GD_HOSTS
        for code in codes:
            for h in hosts:
                urls.append(h.format(code))
        if name in TSFILE_IDS:
            urls.append(TSFILE_HOST.format(TSFILE_IDS[name]))
        cand[name] = urls
    return cand


def discover_candidates():
    """从公开仓库抓最新列表，补充新出现的源"""
    found = {name: [] for name, _g, _c, _r in CHANNELS}
    rx = [(name, re.compile(pattern, re.I)) for name, _g, _c, pattern in CHANNELS]
    for src in DISCOVER_URLS:
        try:
            status, body = http_get(src, timeout=25, limit=20 * 1024 * 1024)
            if status != 200:
                continue
            lines = body.decode("utf-8", "ignore").splitlines()
        except Exception as e:
            print(f"  [发现] 拉取失败 {src.split('/')[-1]}: {e}")
            continue
        got = 0
        for i, line in enumerate(lines):
            if not line.startswith("#EXTINF") or i + 1 >= len(lines):
                continue
            url = lines[i + 1].strip()
            if not url.startswith("http"):
                continue
            title = line.split(",", 1)[-1].strip()
            for name, pattern in rx:
                if not pattern.search(title):
                    continue
                codes = dict((c[0], c[2]) for c in CHANNELS)[name]
                trusted = any(h in url for h in TRUSTED_HOSTS)
                if trusted or any(code in url for code in codes):
                    if url not in found[name]:
                        found[name].append(url)
                        got += 1
        print(f"  [发现] {src.split('/')[-1]}: 新增 {got} 条候选")
    return found


# ---------------------------------------------------------------- 3. 主流程
def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tv-mini.m3u")

    print("== 1/3 构造候选地址 ==")
    cand = build_static_candidates()
    for name, urls in discover_candidates().items():
        cand[name].extend(urls)
    all_urls = sorted({u for urls in cand.values() for u in urls})
    print(f"  候选总数 {len(all_urls)}")

    print("== 2/3 实测可用性与延迟 ==")
    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for url, ms, ok in ex.map(probe, all_urls):
            result[url] = (ms, ok)
    alive = sum(1 for v in result.values() if v[1])
    print(f"  可用 {alive} / {len(all_urls)}")

    print("== 3/3 生成 m3u ==")
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "#EXTM3U",
        "#EXT-X-APTV-LATENCY: FALSE",
        f"# 精简直播源 · 央视+广东 · 自动更新于 {now} · 全部条目均实测可连接",
        "# 每台保留最快的主源 + 备用源（名称带 ·备）；源失效自动被下次扫描剔除",
    ]
    ok_channels, dead_channels = 0, []
    for name, grp, _codes, _rx in CHANNELS:
        good = sorted((result[u][0], u) for u in dict.fromkeys(cand[name]) if result.get(u, (0, False))[1])
        if not good:
            dead_channels.append(name)
            continue
        lines.append(f'#EXTINF:-1 tvg-name="{name}" group-title="{grp}",{name}')
        lines.append(good[0][1])
        for ms, u in good[1:BACKUPS + 1]:
            lines.append(f'#EXTINF:-1 tvg-name="{name}·备" group-title="{grp}",{name}·备')
            lines.append(u)
        ok_channels += 1
        print(f"  ✓ {name:<16} 主 {good[0][0]:>4}ms  {good[0][1].split('/')[2]:<22} 共 {len(good)} 源")
    for name in dead_channels:
        print(f"  ✗ {name:<16} 当前无可用源（下次扫描会自动补回）")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\n完成：{ok_channels}/{len(CHANNELS)} 个频道 → {out_path}")
    # 只要不是全军覆没就认为成功
    return 0 if ok_channels >= len(CHANNELS) - 3 else 1


if __name__ == "__main__":
    sys.exit(main())
